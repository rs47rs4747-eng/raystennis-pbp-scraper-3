#!/usr/bin/env node
/**
 * scrape_pbp_from_uuid_list.js  —  MAIN PBP SCRAPER
 *
 * Reads `/tmp/pbp_matches/_all_uuids.json` (produced by discover_matches_by_date.js)
 * and scrapes every match that isn't already in /tmp/pbp_matches/<uuid>.json.
 *
 * Each set is scraped from a FRESH page load to avoid React/Radix cross-panel
 * interference (expanding Set 1 sections unmounts Set 2/3 panels in headless mode).
 * Set tabs are clicked via Puppeteer native CDP events (element.click() doesn't
 * trigger Radix state changes in headless Chrome).
 *
 * Output: raw set1/set2/set3 text per match, to be parsed by parse_mc_pbp.js.
 *
 * Usage:
 *   node scrape_pbp_from_uuid_list.js                 # default concurrency 4
 *   node scrape_pbp_from_uuid_list.js --concurrency 6
 *   node scrape_pbp_from_uuid_list.js --max 500       # cap this run
 */
const puppeteer = require('puppeteer-extra');
const Stealth = require('puppeteer-extra-plugin-stealth');
puppeteer.use(Stealth());
const fs = require('fs'), path = require('path');

const CHROME = process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium';
const OUT_DIR = process.env.OUT_DIR || '/data';
const UUID_FILE = process.env.UUID_FILE || path.join(OUT_DIR, '_all_uuids.json');

const args = process.argv.slice(2);
const argVal = k => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : null; };
const CONCURRENCY = parseInt(argVal('--concurrency') || '4', 10);
const MAX = parseInt(argVal('--max') || '0', 10);      // 0 = no cap
// --stop-at HH:MM — gracefully stop after current batch finishes (e.g. --stop-at 22:00)
const STOP_AT = argVal('--stop-at');
function pastStopTime() {
  if (!STOP_AT) return false;
  const [h, m] = STOP_AT.split(':').map(Number);
  const now = new Date();
  return now.getHours() > h || (now.getHours() === h && now.getMinutes() >= m);
}

if (!fs.existsSync(UUID_FILE)) {
  console.error(`Missing ${UUID_FILE}. Run discover_matches_by_date.js first.`);
  process.exit(1);
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Find the INNER set panel: the active tab's aria-controls panel, or fallback to
// the innermost visible panel that has sections but does NOT contain other tabpanels.
const FIND_SET_PANEL = `(() => {
  // Method 1: find via active set tab's aria-controls
  const setTabs = [...document.querySelectorAll('button[role="tab"]')]
    .filter(t => /set\\s*\\d/i.test(t.textContent.trim()));
  const activeSetTab = setTabs.find(t => t.getAttribute('data-state') === 'active');
  if (activeSetTab) {
    const panelId = activeSetTab.getAttribute('aria-controls');
    if (panelId) {
      const panel = document.getElementById(panelId);
      if (panel && !panel.hidden) return panelId;
    }
  }
  // Method 2: innermost visible panel with sections (no nested tabpanels)
  const panels = [...document.querySelectorAll('[role="tabpanel"]')];
  const inner = panels.filter(p => !p.hidden && p.querySelectorAll('section.group').length > 0
    && p.querySelectorAll('[role="tabpanel"]').length === 0);
  return inner.length > 0 ? inner[0].id : null;
})()`;

// Expand all collapsed game sections within a specific panel (by ID)
const EXPAND_IN_PANEL = `(async(panelId)=>{
  const wait = ms => new Promise(r => setTimeout(r, ms));
  const activePanel = panelId ? document.getElementById(panelId) : null;
  if (!activePanel) return 'no-panel:' + panelId;
  const scrollH = Math.max(document.body.scrollHeight, 8000);
  for(let y = 0; y <= scrollH; y += 150){ window.scrollTo(0, y); await wait(40); }
  await wait(2000);
  let totalClicked = 0;
  for(let pass = 0; pass < 6; pass++){
    const closed = [...activePanel.querySelectorAll('section.group[data-active="false"]')];
    if(!closed.length) break;
    let passClicked = 0;
    for(const s of closed){
      s.scrollIntoView({ behavior: 'instant', block: 'center' });
      await wait(80);
      const btn = s.querySelector('button.cursor-pointer') || s.querySelector('button');
      if(btn){ btn.click(); passClicked++; totalClicked++; await wait(250); }
    }
    if(passClicked === 0) break;
    await wait(800);
  }
  await wait(500);
  const activeSections = [...activePanel.querySelectorAll('section.group[data-active="true"]')];
  let pbClicked = 0;
  for(const s of activeSections){
    s.scrollIntoView({ behavior: 'instant', block: 'center' });
    await wait(50);
    const innerBtns = [...s.querySelectorAll('button')].filter(b => b.closest('section.group') === s && b.parentElement !== s);
    for(const b of innerBtns){ b.click(); pbClicked++; await wait(80); }
  }
  window.scrollTo(0, 0);
  await wait(500);
  return 'opened=' + totalClicked + ' pb=' + pbClicked;
})`;

// Read content from a specific panel by ID
const READ_PANEL_BY_ID = `((panelId) => {
  const panel = panelId ? document.getElementById(panelId) : null;
  return panel ? panel.innerText : '';
})`;

async function newBrowser() {
  return puppeteer.launch({
    executablePath: CHROME,
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });
}

// Click set N tab using Puppeteer native CDP click and return its aria-controls panel ID.
// Returns the panel ID string on success, or null if the tab wasn't found.
async function clickSetTab(page, n) {
  try {
    const tabs = await page.$$('button[role="tab"]');
    for (const tab of tabs) {
      const text = await tab.evaluate(el => el.textContent.trim());
      if (new RegExp(`set\\s*${n}`, 'i').test(text)) {
        const panelId = await tab.evaluate(el => el.getAttribute('aria-controls'));
        await tab.click(); // Puppeteer native click → real CDP mouse events
        return panelId || null;
      }
    }
  } catch {}
  return null;
}

async function scrapeMatch(browser, uuid, meta, idx, total, existingSaved) {
  const outFile = path.join(OUT_DIR, `${uuid}.json`);
  if (existingSaved.has(uuid)) {
    return { uuid, result: 'skip' };
  }

  const matchUrl = `https://results.tennisdata.com/en/results/${uuid}`;

  // Scrape one set: fresh page load → click set tab → expand → read panel
  async function scrapeOneSet(setNum) {
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');
    const safeEval = async (fn, fb = null) => { try { return await page.evaluate(fn); } catch { return fb; } };
    try {
      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          await page.goto(matchUrl, { waitUntil: 'networkidle2', timeout: 40000 });
          await sleep(1500);
          break;
        } catch { await sleep(1500); }
      }
      const h1 = await safeEval(() => document.querySelector('h1')?.innerText?.trim() || '', '');
      if (!h1 || !/ vs /i.test(h1)) return { h1: '', text: '' };
      // Click set tab if not set 1 (set 1 is default)
      if (setNum > 1) {
        const panelId = await clickSetTab(page, setNum);
        if (!panelId) return { h1, text: '' }; // No set N tab → 2-set match
        await sleep(2500);
      }
      // Find the active set panel, expand, read
      const activePanelId = await safeEval(FIND_SET_PANEL, null);
      if (!activePanelId) return { h1, text: '' };
      await page.evaluate(`(${EXPAND_IN_PANEL})("${activePanelId}")`);
      await sleep(1200);
      const text = await page.evaluate(`(${READ_PANEL_BY_ID})("${activePanelId}")`);
      return { h1, text: (text && typeof text === 'string' && text.length >= 50) ? text : '' };
    } finally {
      await page.close();
    }
  }

  try {
    // --- SET 1 (fresh page) ---
    const s1 = await scrapeOneSet(1);
    if (!s1.h1 || !/ vs /i.test(s1.h1)) return { uuid, result: 'no_title' };
    if (/[\/&]/.test(s1.h1)) return { uuid, result: 'doubles' };
    const [player1, player2] = s1.h1.split(/ vs /i).map(s => s.trim());
    const set1 = s1.text;

    // --- SET 2 (fresh page) ---
    const s2 = await scrapeOneSet(2);
    const set2 = s2.text;

    // --- SET 3 (fresh page, only if set 2 existed) ---
    let set3 = '';
    if (set2) {
      const s3 = await scrapeOneSet(3);
      set3 = s3.text;
    }

    const setCount = [set1, set2, set3].filter(Boolean).length;
    const title = `${player1.split('.').pop()} VS ${player2.split('.').pop()}`.toUpperCase();
    const data = {
      uuid, title, player1, player2,
      tournament: meta.tournament || null,
      match_date: meta.date || null,
      set1: set1 || null,
      set2: set2 || null,
      set3: set3 || null,
      sets_scraped: setCount,
      mc_scraped: true,
      scraped_at: new Date().toISOString(),
    };
    fs.writeFileSync(outFile, JSON.stringify(data, null, 2));
    existingSaved.add(uuid);
    return { uuid, result: 'saved', title, sets: setCount };
  } catch (e) {
    return { uuid, result: 'error', error: e.message };
  }
}

(async () => {
  const allUUIDs = JSON.parse(fs.readFileSync(UUID_FILE, 'utf8'));
  const existingFiles = fs.readdirSync(OUT_DIR).filter(f => f.endsWith('.json') && !f.startsWith('_'));
  const existingSaved = new Set(existingFiles.map(f => f.replace('.json', '')));

  let queue = allUUIDs.filter(u => !existingSaved.has(u.uuid));
  if (MAX > 0) queue = queue.slice(0, MAX);

  console.log(`Discovered UUIDs: ${allUUIDs.length}`);
  console.log(`Already scraped:  ${existingSaved.size}`);
  console.log(`To scrape:        ${queue.length}${MAX > 0 ? ` (capped at ${MAX})` : ''}`);
  console.log(`Concurrency:      ${CONCURRENCY}\n`);

  if (queue.length === 0) { console.log('Nothing to do.'); return; }

  const RECYCLE_EVERY = 40;  // fresh browser every N matches to avoid stale/leak
  let browser = await newBrowser();
  let matchesSinceLaunch = 0;
  let saved = 0, errors = 0, skipped = 0, noTitle = 0, doubles = 0;

  try {
    for (let i = 0; i < queue.length; i += CONCURRENCY) {
      // Recycle browser to prevent stale connections / memory leaks
      if (matchesSinceLaunch >= RECYCLE_EVERY) {
        console.log(`  [recycle] closing browser after ${matchesSinceLaunch} matches...`);
        try { await browser.close(); } catch {}
        await sleep(1500);
        browser = await newBrowser();
        matchesSinceLaunch = 0;
        console.log(`  [recycle] fresh browser ready`);
      }

      const batch = queue.slice(i, i + CONCURRENCY);
      const results = await Promise.all(
        batch.map((m, j) => scrapeMatch(browser, m.uuid, m, i + j + 1, queue.length, existingSaved))
      );
      matchesSinceLaunch += batch.length;
      for (const r of results) {
        const n = queue.findIndex(q => q.uuid === r.uuid) + 1;
        if (r.result === 'saved') {
          saved++;
          process.stdout.write(`  [${n}/${queue.length}] ${r.title} — ${r.sets} set(s)\n`);
        } else if (r.result === 'skip') skipped++;
        else if (r.result === 'no_title') { noTitle++; process.stdout.write(`  [${n}/${queue.length}] ${r.uuid.slice(0,8)} no title\n`); }
        else if (r.result === 'doubles') { doubles++; }
        else if (r.result === 'error') { errors++; process.stdout.write(`  [${n}/${queue.length}] ERR ${r.error?.slice(0,60)}\n`); }
      }
      // Periodic status
      if ((i / CONCURRENCY) % 10 === 0) {
        console.log(`  -- ${i + batch.length}/${queue.length} done (saved=${saved} err=${errors} noTitle=${noTitle} doubles=${doubles}) --`);
      }
      // Graceful stop: finish current batch, then exit cleanly
      if (pastStopTime()) {
        console.log(`\n  [stop-at] Reached ${STOP_AT} — stopping gracefully after completing batch.`);
        break;
      }
    }
  } finally {
    await browser.close();
  }

  console.log(`\n═══════════════════════════════════════════════`);
  console.log(`  Saved:     ${saved}`);
  console.log(`  Skipped:   ${skipped}`);
  console.log(`  No title:  ${noTitle}`);
  console.log(`  Doubles:   ${doubles}`);
  console.log(`  Errors:    ${errors}`);
  console.log(`  Total on disk: ${fs.readdirSync(OUT_DIR).filter(f => f.endsWith('.json') && !f.startsWith('_')).length}`);
  console.log(`═══════════════════════════════════════════════`);
})();
