# PBP Scraper — GitHub Actions

Scrapes tennis PBP from results.tennisdata.com. Runs on GitHub Actions every 6 hours.

Middle chunk (UUIDs 2000-3500 from `_all_uuids.json`), ~1500 matches.
Covers approximately Feb 17 – Mar 17, 2026.

Output: `pbp_matches/<uuid>.json` — committed back to repo after each run.

## Run manually

Go to Actions tab → "Scrape PBP" → "Run workflow".
