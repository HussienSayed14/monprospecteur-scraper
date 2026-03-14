# MonProspecteur Scraper — Client Instructions

All commands below are run from the project folder on the server (where docker-compose.yml is located).

---

## How it works

The system has two Docker containers:

| Container | What it does |
|---|---|
| `scheduler` | Runs 24/7, wakes up every 30s, triggers scraper at 5:00 AM Toronto time |
| `scraper` | Does the actual scraping. Starts when triggered, exits when done |

---

## First-time setup

```bash
# 1. Build the images (only needed once, or after code updates)
docker compose build

# 2. Start the scheduler (runs in background, survives server reboots)
docker compose up -d scheduler

# 3. Verify it's running
docker compose ps
```

---

## Daily automatic run

The scraper runs automatically every day at **5:00 AM Toronto time**.
You don't need to do anything — just make sure the scheduler is running (see "Check status" below).

---

## Manual run — start the scraper right now (not at 5am)

```bash
# Normal run — processes all unread leads
docker compose run --rm scraper

# Test run — processes first 3 leads only (to verify everything works)
docker compose run --rm scraper python main.py --test

# Retry scraping failures from the last run
docker compose run --rm scraper python main.py --retry

# Retry upload failures only (Drive / Sheet)
docker compose run --rm scraper python main.py --retry-uploads
```

---

## Stop the scraper mid-run

If the scraper is currently running and you want to stop it:

```bash
# See running containers
docker ps

# Stop the scraper container (replace CONTAINER_ID with the ID shown above)
docker stop CONTAINER_ID

# Or stop by name
docker stop monprospecteur_scraper
```

The scraper will stop immediately. Any lead that was mid-processing may be incomplete.
Run `python main.py --retry` on the next run to pick those up.

---

## Stop the scheduler (pause automatic daily runs)

```bash
docker compose stop scheduler
```

The scheduler container stops. No more automatic runs until you restart it.

---

## Restart the scheduler

```bash
docker compose start scheduler
```

---

## Restart everything (after a server reboot or code update)

```bash
# Rebuild images if code changed
docker compose build

# Start the scheduler again
docker compose up -d scheduler

# Verify
docker compose ps
```

---

## Check status

```bash
# See which containers are running
docker compose ps

# View scheduler logs (shows when it triggered the scraper)
docker compose logs scheduler

# Follow scheduler logs in real time
docker compose logs -f scheduler

# View last scraper run logs
ls output/logs/
cat output/logs/run_YYYYMMDD_HHMMSS.log
```

---

## Change the scheduled time

Edit the `.env` file and change:

```
SCHEDULE_HOUR=5      # 24-hour format, Toronto time
SCHEDULE_MINUTE=0
```

Then restart the scheduler:

```bash
docker compose restart scheduler
```

---

## Output files

All output lands in the `output/` folder on the server:

```
output/
  pdfs/      ← notarial act PDFs
  prints/    ← print page PDFs
  data/      ← raw JSON, Excel files, run stats
  logs/      ← one .log file per run + scheduler.log
  failed/    ← retry queues (if any failures)
```

---

## If the scraper stops working

1. Check the log file in `output/logs/` for the last run
2. Check if the session expired: `docker compose run --rm scraper python main.py --test`
3. If you see a Google Auth error in the email: delete `token.json` from the project folder, then run `python sheets_uploader.py` on your local machine to re-authenticate
4. If the proxy stopped working: verify Webshare credentials in `.env`