# swiggy-lead-sync-runner

Runner code for the Swiggy Dineout -> SquadStack lead sync.

- This repo is public and holds code only.
- All lead data (names, phone numbers, archive) lives in a separate
  private repo and is never committed here. `.gitignore` blocks it.
- Runs are triggered by a Cloudflare Worker (cron + Slack buttons) via
  `repository_dispatch`. There are no `pull_request` triggers, so forks
  cannot run these workflows or read their secrets.
