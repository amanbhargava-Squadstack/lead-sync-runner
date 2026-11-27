#!/usr/bin/env python3
"""
Swiggy Dineout (SFTP) -> SquadStack lead-upserted automation

What this does:
1. Connects to the SFTP drop and reads the bookings CSVs from BOTH the
   new directory (/ANALYTICS.PUBLIC.DO_BOOKINGS_SQUADSTACK) and the old
   one (/analytics.public.do_bookings_Squadstack_3). A missing or empty
   directory is logged and skipped; the run only fails if neither has
   any CSV.
2. Transforms each row per the mapping rules you gave (adds
   booking_slottime, internal_id, famous_for, lead_name, contact_name,
   campaign_id, number).
3. POSTs each row to the SquadStack lead-upserted endpoint.
4. Logs successes/failures and tracks which rows were already pushed,
   so re-runs don't create duplicate leads. A booking that shows up in
   both directories is handled once per run (BOOKING_ORDER_ID dedup).
5. No daily cap - every new lead found is pushed (cap removed).
6. Archives every new lead seen each day - with full payload data and
   outcome (pushed/capped/failed) - into leads_archive/<date>.csv, plus
   a running leads_archive/daily_counts.csv summary. Both are meant to
   be committed back to the repo (see the GitHub Actions workflow) so
   they persist as a permanent record, unlike swiggy_sync_data/ which
   is just a cache.

Requires: paramiko, requests
    pip install paramiko requests

Payload schema (confirmed against SquadStack docs + campaign field list):
    lead_id, phone_number, contact_name are the fixed keys; the rest
    (booking_date, booking_slottime, bookingfee, cost_for_two, covers,
    deal_category, famous_for, primary_cuisine, restaurant_locality,
    restaurant_name, select_ambience_tags, offer_name, nu_ru_flag,
    reservation_type, lead_name, campaign_id) are sent as named fields
    per the campaign's field list. lead_id is the lifetime unique
    identifier per lead (= BOOKING_ORDER_ID here) and must stay stable
    across re-upserts of the same booking.
"""

import csv
import io
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import paramiko
import requests

IST = ZoneInfo("Asia/Kolkata")

# ----------------------------------------------------------------------------
# CONFIG - fill in / move to env vars before running
# ----------------------------------------------------------------------------

SFTP_HOST = "s-1c7faf60c90c42689.server.transfer.ap-south-1.amazonaws.com"
SFTP_PORT = 22
SFTP_USERNAME = "sftp-squadstack"
SFTP_PRIVATE_KEY_PATH = os.environ.get("SFTP_PRIVATE_KEY_PATH", "swiggy_sftp_key.pem")

# Every directory checked on each run, in priority order. The new
# directory comes first; the old one stays in the list until Swiggy
# confirms nothing lands there anymore. Remove it from here when that
# happens - no other code change needed.
SFTP_REMOTE_PATHS = [
    "/ANALYTICS.PUBLIC.DO_BOOKINGS_SQUADSTACK",   # new (current)
    "/analytics.public.do_bookings_Squadstack_3",  # old (legacy)
]

# Local dir to keep a copy of each downloaded CSV + a "seen ids" ledger
# so re-runs are idempotent. This is ephemeral/cache-only - NOT committed
# to the repo (see .gitignore in the setup guide).
WORK_DIR = Path("./swiggy_sync_data")
WORK_DIR.mkdir(exist_ok=True)
SEEN_IDS_FILE = WORK_DIR / "seen_booking_ids.txt"
DAILY_STATE_FILE = WORK_DIR / "daily_cap_state.json"

# Persistent archive - this DOES get committed back to the repo by the
# GitHub Actions workflow, so it becomes a permanent, date-stamped
# record of every lead that came in and what happened to it.
ARCHIVE_DIR = Path("./leads_archive")
ARCHIVE_DIR.mkdir(exist_ok=True)
DAILY_SUMMARY_FILE = ARCHIVE_DIR / "daily_counts.csv"
FAILED_LEADS_FILE = ARCHIVE_DIR / "failed_leads.csv"
CAP_OVERRIDES_FILE = ARCHIVE_DIR / "cap_overrides.json"
CAP_ALERTS_FILE = ARCHIVE_DIR / "cap_alerts_sent.json"
PIPELINE_STATUS_FILE = ARCHIVE_DIR / "pipeline_status.json"
# Tracks, per hourly batch, whether any run found new leads and whether
# the one "no new leads this hour" message has gone out (committed).
HOUR_STATE_FILE = ARCHIVE_DIR / "hour_state.json"

# Which scheduled tick started this run, passed in by the Cloudflare
# Worker via repository_dispatch client_payload.slot:
#   "first"    -> the :45 run   (silent when it finds nothing new)
#   "followup" -> the :50 run (and any later tick in the same hour)
#   ""         -> manual run    (old behaviour: always posts)
RUN_SLOT = os.environ.get("RUN_SLOT", "").strip().lower()

API_ENDPOINT = "https://api.squadstack.com/voice/crm-integration-v2/54WJG01/lead-upserted"
API_TOKEN = os.environ.get("SQUADSTACK_API_TOKEN")  # set this env var before running - don't hardcode the token here
CAMPAIGN_ID = "9527"

DAILY_LEAD_CAP = 1000  # NOT enforced by main() anymore (cap removed) - kept only so check/cap helper scripts still import cleanly
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")  # set this env var to enable Slack alerts
# Optional: webhook for the owner's private DM. When set, Pause/Stop/Start
# buttons go ONLY there and the shared channel gets button-free messages.
SLACK_CONTROL_WEBHOOK_URL = os.environ.get("SLACK_CONTROL_WEBHOOK_URL")

if not API_TOKEN:
    sys.exit("Set SQUADSTACK_API_TOKEN as an environment variable before running this script.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(WORK_DIR / "sync.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("swiggy_lead_sync")


# ----------------------------------------------------------------------------
# STEP 1: Pull the CSVs from SFTP (both directories)
# ----------------------------------------------------------------------------

def _list_csv_files(sftp, remote_dir: str) -> list:
    """
    Returns the CSV entries inside remote_dir, sorted by name. Prefers
    Spark-style 'part-*.csv' files; if the directory has none of those
    but does have other .csv files (the new export may name them
    differently), those are used instead. Returns [] if the directory
    is missing, unreadable, or has no CSVs - the caller decides whether
    that's fatal.
    """
    try:
        entries = sftp.listdir_attr(remote_dir)
    except (FileNotFoundError, IOError, OSError) as e:
        log.warning("Could not list %s (missing or no access): %s", remote_dir, e)
        return []

    csv_entries = [e for e in entries if e.filename.lower().endswith(".csv")]
    part_files = [e for e in csv_entries if e.filename.startswith("part-")]
    chosen = part_files or csv_entries

    if not chosen:
        log.info("No CSV files in %s - directory contents: %s", remote_dir, [e.filename for e in entries])
    elif not part_files:
        log.info("No part-*.csv in %s, using other CSV files instead: %s",
                 remote_dir, [e.filename for e in chosen])

    return sorted(chosen, key=lambda e: e.filename)


def _normalize_row(row: dict) -> dict:
    """
    Upper-cases and trims column names so both directories map onto the
    same keys (BOOKING_ORDER_ID, PHONE, NAME, ...) even if the new
    export changes header casing or adds stray whitespace.
    """
    return {(k or "").strip().upper(): v for k, v in row.items() if k is not None}


def fetch_csv_from_sftp() -> tuple:
    """
    Connects over SFTP once and returns (rows, newest_file_mtime_ist):
      - rows: list of dicts (normalized upper-case headers) from every
        CSV found across ALL directories in SFTP_REMOTE_PATHS, new
        directory first.
      - newest_file_mtime_ist: datetime of the most recently modified
        file across all directories, used by main() to warn when no
        fresh export has landed today.

    Each file is parsed on its own with its own header, so the old and
    new exports can have different column orders without corrupting
    each other's rows. Raises FileNotFoundError only if NO directory
    yields a CSV.
    """
    key = paramiko.Ed25519Key.from_private_key_file(SFTP_PRIVATE_KEY_PATH)

    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USERNAME, pkey=key)
    sftp = paramiko.SFTPClient.from_transport(transport)

    all_rows = []
    newest_mtime_epoch = None
    files_read = 0
    run_stamp = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"

    try:
        for remote_dir in SFTP_REMOTE_PATHS:
            log.info("Connected to SFTP, listing %s", remote_dir)
            csv_files = _list_csv_files(sftp, remote_dir)
            if not csv_files:
                continue

            log.info("Found %s data file(s) in %s: %s",
                     len(csv_files), remote_dir, [e.filename for e in csv_files])

            dir_rows = 0
            for entry in csv_files:
                if entry.st_mtime and (newest_mtime_epoch is None or entry.st_mtime > newest_mtime_epoch):
                    newest_mtime_epoch = entry.st_mtime

                remote_path = f"{remote_dir.rstrip('/')}/{entry.filename}"
                with sftp.open(remote_path, "r") as remote_file:
                    content = remote_file.read()
                    if isinstance(content, bytes):
                        content = content.decode("utf-8-sig")  # -sig strips a BOM if present
                    else:
                        content = content.lstrip("\ufeff")

                if not content.strip():
                    continue

                file_rows = [_normalize_row(r) for r in csv.DictReader(io.StringIO(content))]
                all_rows.extend(file_rows)
                dir_rows += len(file_rows)
                files_read += 1

                # timestamped local copy for audit/debugging, tagged by source dir
                dir_tag = remote_dir.strip("/").replace("/", "_").replace(".", "_")
                local_copy = WORK_DIR / f"bookings_{run_stamp}_{dir_tag}_{entry.filename}"
                local_copy.write_text(content)

            log.info("Read %s row(s) from %s", dir_rows, remote_dir)

        if files_read == 0:
            raise FileNotFoundError(
                f"No CSV file found in any SFTP directory: {SFTP_REMOTE_PATHS}"
            )

        newest_mtime_ist = datetime.fromtimestamp(newest_mtime_epoch, tz=IST) if newest_mtime_epoch else None
        log.info("Total rows read across %s file(s): %s", files_read, len(all_rows))
        return all_rows, newest_mtime_ist
    finally:
        sftp.close()
        transport.close()


def extract_first_name(full_name: str) -> str:
    """
    Extracts the first name from a full name string:
      - Normally, the first word is the first name.
      - If the first word is a title (Mr, Mr., Dr, Dr., Ms, Ms., Mrs,
        Mrs., Miss, Miss. - case-insensitive), the SECOND word is used
        as the first name instead.
    e.g. "Rohan Sharma" -> "Rohan"
         "Mr. Rohan Sharma" -> "Rohan"
         "DR Priya Nair" -> "Priya"
    """
    name = (full_name or "").strip()
    if not name:
        return ""

    words = name.split()
    titles = {"mr", "mr.", "dr", "dr.", "ms", "ms.", "mrs", "mrs.", "miss", "miss."}

    if words[0].lower() in titles and len(words) > 1:
        return words[1]
    return words[0]


# ----------------------------------------------------------------------------
# STEP 2: Transform rows per your mapping rules
# ----------------------------------------------------------------------------

def transform_row(row: dict) -> dict:
    """
    Takes a raw CSV row (source headers) and returns the enriched row
    with the derived columns you specified:
      - booking_slottime = time portion of BOOKING_DATETIME
      - internal_id       = BOOKING_ORDER_ID
      - famous_for        = TOP_DISHES
      - lead_name         = first name extracted from NAME (title-aware)
      - contact_name      = same first name extracted from NAME
      - campaign_id       = constant 9527
      - number            = PHONE
    """
    enriched = dict(row)  # keep all original columns

    booking_datetime = (row.get("BOOKING_DATETIME") or "").strip()
    slot_time = ""
    if booking_datetime:
        try:
            # Expected format: "2026-08-27 16:30:00"
            dt = datetime.strptime(booking_datetime, "%Y-%m-%d %H:%M:%S")
            slot_time = dt.strftime("%H:%M:%S")
        except ValueError:
            # Fallback: just take whatever's after the space
            parts = booking_datetime.split(" ")
            slot_time = parts[1] if len(parts) > 1 else ""
            log.warning(
                "Could not parse BOOKING_DATETIME '%s' with expected format; "
                "used naive split fallback -> '%s'", booking_datetime, slot_time
            )

    first_name = extract_first_name(row.get("NAME", ""))

    enriched["booking_slottime"] = slot_time
    enriched["internal_id"] = row.get("BOOKING_ORDER_ID", "")
    enriched["famous_for"] = row.get("TOP_DISHES", "")
    enriched["lead_name"] = first_name
    enriched["contact_name"] = first_name
    enriched["campaign_id"] = CAMPAIGN_ID
    enriched["number"] = row.get("PHONE", "")

    return enriched


# ----------------------------------------------------------------------------
# STEP 3: Build API payload + push to SquadStack
# ----------------------------------------------------------------------------

def build_payload(row: dict) -> dict:
    """
    Confirmed schema (from SquadStack lead-upserted docs + campaign field
    list you shared): lead_id, phone_number, contact_name are the fixed
    keys; everything else is sent as a named custom field (the campaign
    accepts custom fields under their own field names rather than the
    generic custom_field1/custom_field2 shown in the generic docs).

    Mandatory on first upsert for a given lead_id: phone_number, contact_name.
    """
    payload = {
        "lead_id": row["internal_id"],            # = BOOKING_ORDER_ID
        "phone_number": row["number"],             # = PHONE
        "contact_name": row["contact_name"],       # = first name from NAME
        "lead_name": row["lead_name"],              # = first name from NAME
        "campaign_id": row["campaign_id"],          # constant "9527"
        "booking_date": row.get("BOOKING_DATE", ""),
        "booking_slottime": row.get("booking_slottime", ""),
        "bookingfee": row.get("BOOKINGFEE", ""),
        "cost_for_two": row.get("COST_FOR_TWO", ""),
        "covers": row.get("COVERS", ""),
        "deal_category": row.get("DEAL_CATEGORY", ""),
        "famous_for": row.get("famous_for", ""),      # = TOP_DISHES
        "primary_cuisine": row.get("PRIMARY_CUISINE", ""),
        "restaurant_locality": row.get("RESTAURANT_LOCALITY", ""),
        "restaurant_name": row.get("RESTAURANT_NAME", ""),
        "select_ambience_tags": row.get("SELECT_AMBIENCE_TAGS", ""),
        "offer_name": row.get("OFFER_NAME", ""),
        "nu_ru_flag": row.get("NU_RU_FLAG", ""),
        "reservation_type": row.get("RESERVATION_TYPE", ""),
    }
    return payload


def push_lead(payload: dict, session: requests.Session) -> tuple:
    """Returns (success: bool, error_detail: str) - error_detail is '' on success."""
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }
    lead_id = payload.get("lead_id")
    # PUBLIC LOGS: this runs in a public repo, so logs must never contain
    # names or phone numbers. Only lead_id and HTTP status are logged; the
    # full error text goes to failed_leads.csv in the PRIVATE data repo.
    try:
        resp = session.post(API_ENDPOINT, headers=headers, json=payload, timeout=15)
        if resp.status_code == 200:
            log.info("Lead pushed OK (lead_id=%s)", lead_id)
            return True, ""

        # Documented 400 cases mean "this lead is done, don't keep retrying it"
        # rather than a real integration failure - treat as a handled skip
        # so we still mark it seen and don't spam retries every run.
        if resp.status_code == 400:
            body_lower = resp.text.lower()
            if "already been marked as converted" in body_lower or "unjudged leads cannot be re-activated" in body_lower:
                log.info("Lead lead_id=%s already finalized on SquadStack's side, marking as seen.", lead_id)
                return True, ""  # treated as "handled" so it's added to seen_ids and not retried

        error_detail = f"HTTP {resp.status_code}: {resp.text[:300]}"
        log.error("Lead push FAILED (lead_id=%s) -> HTTP %s (details in private failed_leads.csv)",
                  lead_id, resp.status_code)
        return False, error_detail
    except requests.RequestException as e:
        error_detail = f"Request error: {e}"
        log.error("Request error pushing lead lead_id=%s: %s", lead_id, type(e).__name__)
        return False, error_detail


# ----------------------------------------------------------------------------
# STEP 4: Idempotency helpers (avoid re-pushing the same booking)
# ----------------------------------------------------------------------------

def load_seen_ids() -> set:
    """
    Builds the set of already-pushed lead IDs (= BOOKING_ORDER_ID) from
    TWO sources, combined:
      1. The cache file (swiggy_sync_data/seen_booking_ids.txt) - fast,
         but only survives as long as GitHub's Actions cache does.
      2. The git-committed leads_archive/*.csv files - the durable
         source of truth, since it's permanent repo history and can't
         be silently evicted like a cache can.
    This means even if the cache disappears, a lead that's already in
    the archive as "pushed" will still never be pushed again.
    """
    seen = set()

    if SEEN_IDS_FILE.exists():
        seen.update(SEEN_IDS_FILE.read_text().splitlines())

    if ARCHIVE_DIR.exists():
        for archive_file in ARCHIVE_DIR.glob("*.csv"):
            if archive_file.name in (DAILY_SUMMARY_FILE.name, FAILED_LEADS_FILE.name):
                continue
            try:
                with open(archive_file, "r", newline="") as f:
                    for row in csv.DictReader(f):
                        # Both "pushed" and "not_pushed" are treated as
                        # already-handled - so resuming after a pause never
                        # auto-pushes leads that accumulated as not_pushed.
                        # Those leads stay in the archive as not_pushed
                        # permanently unless you explicitly run "Repush
                        # Capped Leads" (which also handles not_pushed).
                        if row.get("status") in ("pushed", "not_pushed") and row.get("lead_id"):
                            seen.add(row["lead_id"])
            except (OSError, csv.Error) as e:
                log.warning("Could not read archive file %s while building seen-ids: %s", archive_file, e)

    return seen


def append_seen_id(booking_id: str):
    with open(SEEN_IDS_FILE, "a") as f:
        f.write(booking_id + "\n")


def load_already_logged_not_pushed_ids() -> set:
    """
    Lead IDs already archived with status "not_pushed" (from a previous
    paused run). Checked so a multi-hour or multi-day pause doesn't
    write a fresh duplicate "not_pushed" row for the same lead on every
    single run - each lead gets logged once in that state, not once per
    run it happens to still be sitting unpushed.
    """
    logged = set()
    if not ARCHIVE_DIR.exists():
        return logged
    for archive_file in ARCHIVE_DIR.glob("*.csv"):
        if archive_file.name in (DAILY_SUMMARY_FILE.name, FAILED_LEADS_FILE.name):
            continue
        try:
            with open(archive_file, "r", newline="") as f:
                for row in csv.DictReader(f):
                    if row.get("status") == "not_pushed" and row.get("lead_id"):
                        logged.add(row["lead_id"])
        except (OSError, csv.Error) as e:
            log.warning("Could not read archive file %s while building not-pushed log: %s", archive_file, e)
    return logged


def load_previously_failed_ids() -> set:
    """
    Lead IDs archived with status "failed" by an earlier run. These leads
    are still retried on every run, but a retry that fails AGAIN is not
    news: it doesn't count as a new lead for Slack, and isn't written to
    the archive / failed_leads.csv a second time.
    """
    failed_ids = set()
    if not ARCHIVE_DIR.exists():
        return failed_ids
    for archive_file in ARCHIVE_DIR.glob("*.csv"):
        if archive_file.name in (DAILY_SUMMARY_FILE.name, FAILED_LEADS_FILE.name):
            continue
        try:
            with open(archive_file, "r", newline="") as f:
                for row in csv.DictReader(f):
                    if row.get("status") == "failed" and row.get("lead_id"):
                        failed_ids.add(row["lead_id"])
        except (OSError, csv.Error) as e:
            log.warning("Could not read archive file %s while building failed-ids: %s", archive_file, e)
    return failed_ids


# ----------------------------------------------------------------------------
# STEP 5: Daily lead cap (checked once per run, not per lead - see below)
# ----------------------------------------------------------------------------

def get_archive_pushed_count(date_str: str) -> int:
    """
    Reads the durable running total of leads actually pushed today from
    leads_archive/daily_counts.csv (committed by previous runs). This is
    the authoritative count - used both to recover daily_state if the
    Actions cache is ever lost, and as the basis for cap-increase math.
    """
    if not DAILY_SUMMARY_FILE.exists():
        return 0
    try:
        with open(DAILY_SUMMARY_FILE, "r", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("date") == date_str:
                    return int(row.get("pushed", 0))
    except (OSError, csv.Error, ValueError):
        pass
    return 0


def load_daily_state() -> dict:
    """
    Returns {"date": "YYYY-MM-DD", "count": N} for today (IST).
    If the stored state is from a previous day OR missing (e.g. cache
    was evicted), count is backfilled from the durable archive summary
    rather than blindly reset to 0, so a lost cache can't cause the cap
    to be silently ignored mid-day.
    """
    today_str = datetime.now(IST).strftime("%Y-%m-%d")

    if DAILY_STATE_FILE.exists():
        try:
            state = json.loads(DAILY_STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            state = {}
    else:
        state = {}

    if state.get("date") != today_str:
        recovered_count = get_archive_pushed_count(today_str)
        state = {"date": today_str, "count": recovered_count}
        log.info("New day or lost cache (%s) - daily count set from archive: %s", today_str, recovered_count)

    return state


def save_daily_state(state: dict):
    DAILY_STATE_FILE.write_text(json.dumps(state))


def get_effective_cap(date_str: str) -> int:
    """
    Returns today's effective cap. leads_archive/cap_overrides.json holds
    an ABSOLUTE cap value per date (not an add-on to DAILY_LEAD_CAP) - it's
    written by apply_cap_increase.py as "current actual pushed count at
    the moment of the Slack click" + the chosen increment. If no override
    exists for the date, the base DAILY_LEAD_CAP applies.
    """
    if not CAP_OVERRIDES_FILE.exists():
        return DAILY_LEAD_CAP
    try:
        overrides = json.loads(CAP_OVERRIDES_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return DAILY_LEAD_CAP
    if date_str in overrides:
        return int(overrides[date_str])
    return DAILY_LEAD_CAP


def has_alert_been_sent(date_str: str) -> bool:
    """Durable (committed) check, so a lost cache can't cause repeat alerts."""
    return get_alert_sent_at(date_str) is not None


def get_alert_sent_at(date_str: str):
    """
    Returns the datetime the cap-hit alert was sent for date_str, or
    None if no alert is currently pending (either none was sent, or a
    button click already cleared it - see apply_cap_increase.py). Used
    to auto-pause if too much time passes with no response.
    """
    if not CAP_ALERTS_FILE.exists():
        return None
    try:
        alerts = json.loads(CAP_ALERTS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    entry = alerts.get(date_str)
    if not entry:
        return None
    sent_at_str = entry.get("sent_at") if isinstance(entry, dict) else None
    if not sent_at_str:
        return None
    try:
        return datetime.fromisoformat(sent_at_str)
    except ValueError:
        return None


def mark_alert_sent(date_str: str):
    alerts = {}
    if CAP_ALERTS_FILE.exists():
        try:
            alerts = json.loads(CAP_ALERTS_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            alerts = {}
    alerts[date_str] = {"sent_at": datetime.now(IST).isoformat()}
    CAP_ALERTS_FILE.write_text(json.dumps(alerts, indent=2, sort_keys=True))


def send_slack_alert(message: str):
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set - skipping Slack alert: %s", message)
        return
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
        if resp.status_code != 200:
            log.error("Slack alert failed -> %s | %s", resp.status_code, resp.text[:300])
        else:
            log.info("Slack alert sent.")
    except requests.RequestException as e:
        log.error("Slack alert request error: %s", e)


def send_slack_cap_hit_alert(date_str: str, actual_count: int, cap: int):
    """
    Sends the cap-hit alert WITH one-click buttons to raise today's cap.
    actual_count is the REAL total pushed today (which may already be
    over `cap`, since a run is allowed to finish its full batch even if
    it crosses the cap mid-way - see main()). Each button's value is
    picked up by the Cloudflare Worker bridge, which triggers the
    'Apply Cap Increase' workflow via repository_dispatch; that workflow
    computes the new cap as (actual pushed count AT THE TIME OF THE
    CLICK) + the chosen increment, not the original base cap.
    """
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set - skipping cap-hit Slack alert.")
        return

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":rotating_light: *Swiggy lead sync: daily cap reached for {date_str} (IST).*\n"
                    f"Cap: *{cap}*  |  Actual leads pushed today: *{actual_count}*\n"
                    f"Further new leads today will not be pushed until the cap resets tomorrow "
                    f"— unless you raise it now.\n"
                    f":alarm_clock: No response in 50 minutes auto-pauses the sync for the rest "
                    f"of today (resumes on its own tomorrow)."
                ),
            },
        },
        {
            "type": "actions",
            "block_id": "cap_increase_actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "+500"},
                    "value": f"increase:{date_str}:500",
                    "action_id": "increase_cap_500",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "+1000"},
                    "value": f"increase:{date_str}:1000",
                    "action_id": "increase_cap_1000",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "+1500"},
                    "value": f"increase:{date_str}:1500",
                    "action_id": "increase_cap_1500",
                },
            ],
        },
    ]
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=10)
        if resp.status_code != 200:
            log.error("Slack cap-hit alert failed -> %s | %s", resp.status_code, resp.text[:300])
        else:
            log.info("Slack cap-hit alert (with buttons) sent.")
    except requests.RequestException as e:
        log.error("Slack cap-hit alert request error: %s", e)


def send_slack_message_with_control_buttons(text: str):
    """
    Sends a message with three buttons attached, so the pipeline can be
    controlled straight from Slack without opening GitHub:
      - "Pause Today"  -> pauses for the rest of today only, auto-resumes tomorrow
      - "Stop Sync"    -> pauses indefinitely, stays off until explicitly resumed
      - "Start Sync"   -> resumes (clears either kind of pause)
    Picked up by the Cloudflare Worker bridge, which triggers the
    'Pause Sync' / 'Resume Sync' GitHub Actions workflows via
    repository_dispatch (same pattern as the cap-increase buttons).

    BUTTON VISIBILITY: if SLACK_CONTROL_WEBHOOK_URL is set (a webhook
    pointing at the owner's own DM), the channel gets the same text with
    NO buttons, and the copy WITH buttons goes only to that DM - so
    view-only people in the channel never see the controls. If it's not
    set, buttons are posted in the channel as before.
    """
    if SLACK_CONTROL_WEBHOOK_URL:
        send_slack_alert(text)  # channel copy, no buttons
        button_target = SLACK_CONTROL_WEBHOOK_URL
    else:
        button_target = SLACK_WEBHOOK_URL
    if not button_target:
        log.warning("SLACK_WEBHOOK_URL not set - skipping Slack message: %s", text)
        return

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        {
            "type": "actions",
            "block_id": "pipeline_control_actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "⏸ Pause Today"},
                    "value": "pause_sync_today",
                    "action_id": "pause_sync_today",
                    "confirm": {
                        "title": {"type": "plain_text", "text": "Pause for today?"},
                        "text": {"type": "mrkdwn", "text": "No leads pushed for the rest of today - resumes automatically tomorrow."},
                        "confirm": {"type": "plain_text", "text": "Yes, pause today"},
                        "deny": {"type": "plain_text", "text": "Cancel"},
                    },
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "🛑 Stop Sync"},
                    "style": "danger",
                    "value": "pause_sync_indefinite",
                    "action_id": "pause_sync_indefinite",
                    "confirm": {
                        "title": {"type": "plain_text", "text": "Stop the sync indefinitely?"},
                        "text": {"type": "mrkdwn", "text": "This stays off across days - no leads pushed until you explicitly click Start Sync."},
                        "confirm": {"type": "plain_text", "text": "Yes, stop it"},
                        "deny": {"type": "plain_text", "text": "Cancel"},
                    },
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "▶️ Start Sync"},
                    "style": "primary",
                    "value": "resume_sync",
                    "action_id": "resume_sync",
                },
            ],
        },
    ]
    try:
        resp = requests.post(button_target, json={"blocks": blocks}, timeout=10)
        if resp.status_code != 200:
            log.error("Slack control-button message failed -> %s | %s", resp.status_code, resp.text[:300])
        else:
            log.info("Slack control-button message sent.")
    except requests.RequestException as e:
        log.error("Slack control-button message request error: %s", e)


# ----------------------------------------------------------------------------
# STEP 6: Persistent per-day archive (committed to the repo)
# ----------------------------------------------------------------------------

# Fixed column order for the per-day archive files - the full payload
# sent to SquadStack, plus a status column for what happened to it.
# Must contain every key build_payload() returns, or archiving fails.
ARCHIVE_COLUMNS = [
    "status", "lead_id", "phone_number", "contact_name", "lead_name",
    "campaign_id", "booking_date", "booking_slottime", "bookingfee",
    "cost_for_two", "covers", "deal_category", "famous_for",
    "primary_cuisine", "restaurant_locality", "restaurant_name",
    "select_ambience_tags", "offer_name", "nu_ru_flag", "reservation_type",
]


def _upgrade_csv_header(path: Path, fieldnames: list):
    """
    If an existing CSV was written with an older column list, rewrite it
    with the current header so new rows line up. Old rows get '' for any
    new columns. No-op when the header already matches.
    """
    if not path.exists():
        return
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames == fieldnames:
            return
        old_rows = list(reader)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in old_rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def archive_lead_row(date_str: str, payload: dict, status: str):
    """
    Appends one row to leads_archive/<date>.csv - one file per day,
    holding every new lead seen that day (pushed or failed) along with
    the exact data sent. Writes the header once per file.
    """
    archive_file = ARCHIVE_DIR / f"{date_str}.csv"
    _upgrade_csv_header(archive_file, ARCHIVE_COLUMNS)
    is_new_file = not archive_file.exists()

    row = {"status": status, **payload}

    with open(archive_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ARCHIVE_COLUMNS)
        if is_new_file:
            writer.writeheader()
        writer.writerow(row)


# Single running file across all days - just the failures, with the
# actual error reason attached, so you can check one place instead of
# digging through sync.log or a specific day's full archive file.
FAILED_LEADS_COLUMNS = ["date", "error"] + ARCHIVE_COLUMNS[1:]  # drop "status" (always "failed" here)


def append_failed_lead(date_str: str, payload: dict, error_detail: str):
    """Appends one row to leads_archive/failed_leads.csv - a single, all-time running log of failures only."""
    _upgrade_csv_header(FAILED_LEADS_FILE, FAILED_LEADS_COLUMNS)
    is_new_file = not FAILED_LEADS_FILE.exists()

    row = {"date": date_str, "error": error_detail, **payload}

    with open(FAILED_LEADS_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FAILED_LEADS_COLUMNS)
        if is_new_file:
            writer.writeheader()
        writer.writerow(row)


def update_daily_summary(date_str: str, new_leads: int, pushed: int, capped: int, failed: int, not_pushed: int = 0):
    """
    Maintains leads_archive/daily_counts.csv - one row per date with
    running totals for that day. Since this script may run several
    times a day, it accumulates onto today's existing row rather than
    overwriting it, so the totals reflect the full day, not just the
    latest run.
    """
    rows = {}
    if DAILY_SUMMARY_FILE.exists():
        with open(DAILY_SUMMARY_FILE, "r", newline="") as f:
            for r in csv.DictReader(f):
                rows[r["date"]] = r

    existing = rows.get(date_str, {
        "date": date_str, "total_new_leads": "0", "pushed": "0",
        "capped": "0", "failed": "0", "not_pushed": "0",
    })
    existing.setdefault("not_pushed", "0")  # older rows won't have this column yet
    existing["total_new_leads"] = str(int(existing["total_new_leads"]) + new_leads)
    existing["pushed"] = str(int(existing["pushed"]) + pushed)
    existing["capped"] = str(int(existing["capped"]) + capped)
    existing["failed"] = str(int(existing["failed"]) + failed)
    existing["not_pushed"] = str(int(existing["not_pushed"]) + not_pushed)
    rows[date_str] = existing

    with open(DAILY_SUMMARY_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "total_new_leads", "pushed", "capped", "failed", "not_pushed"])
        writer.writeheader()
        for d in sorted(rows.keys()):
            writer.writerow(rows[d])


# ----------------------------------------------------------------------------
# Per-hour Slack noise control (for the :45 / :50 double run)
# ----------------------------------------------------------------------------

def current_batch_hour() -> str:
    """
    The hourly batch this run belongs to, as "YYYY-MM-DD HH" (IST).
    Shifted back 30 min so a run at :45, :50 (or any later tick before
    the next hour's :30) maps to the same batch - the drop that lands
    around :35-:47.
    """
    return (datetime.now(IST) - timedelta(minutes=30)).strftime("%Y-%m-%d %H")


def load_hour_state(batch_hour: str) -> dict:
    state = {}
    if HOUR_STATE_FILE.exists():
        try:
            state = json.loads(HOUR_STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            state = {}
    if state.get("batch_hour") != batch_hour:
        state = {"batch_hour": batch_hour, "new_leads": 0, "quiet_notified": False}
    return state


def save_hour_state(state: dict):
    HOUR_STATE_FILE.write_text(json.dumps(state, indent=2))


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------

def is_pipeline_paused() -> dict:
    """
    Reads leads_archive/pipeline_status.json - committed to the repo, so
    it's checked BEFORE this run touches SFTP or the SquadStack API at
    all. Toggled via the 'Pause Sync' / 'Resume Sync' GitHub Actions
    workflows, or the Slack Pause Today / Stop Sync / Start Sync buttons.

    A pause set with an "auto_pause_date" (i.e. the "Pause Today" kind,
    not "Stop Sync") self-expires the moment the
    date rolls over - so it resumes on its own the next day, unlike a
    manual pause which stays off until someone explicitly resumes it.

    Returns {"paused": bool, "reason": str, "updated_at": str} - all
    fields default safely if the file doesn't exist (not paused).
    """
    if not PIPELINE_STATUS_FILE.exists():
        return {"paused": False, "reason": "", "updated_at": ""}
    try:
        status = json.loads(PIPELINE_STATUS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {"paused": False, "reason": "", "updated_at": ""}

    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    auto_pause_date = status.get("auto_pause_date")
    if status.get("paused") and auto_pause_date and auto_pause_date != today_str:
        cleared = {"paused": False, "reason": "", "updated_at": datetime.now(IST).strftime("%Y-%m-%d %I:%M %p IST")}
        PIPELINE_STATUS_FILE.write_text(json.dumps(cleared, indent=2))
        log.info("Yesterday's auto-pause (cap timeout) has expired - resuming automatically for the new day.")
        return {"paused": False, "reason": "", "updated_at": ""}

    return {
        "paused": bool(status.get("paused", False)),
        "reason": status.get("reason", ""),
        "updated_at": status.get("updated_at", ""),
    }


def maybe_auto_pause_for_cap_timeout():
    """
    If a cap-hit alert was sent today and 50+ minutes have passed with
    no response (i.e. has_alert_been_sent is still true - a button
    click would have cleared it), auto-pauses the pipeline for the rest
    of today. This stops leads from piling up as "capped" run after run
    with no resolution. Marked with auto_pause_date so it self-expires
    tomorrow (see is_pipeline_paused above), unlike a manual Stop Sync.
    """
    if is_pipeline_paused()["paused"]:
        return  # already paused (manual or auto) - nothing to do

    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    sent_at = get_alert_sent_at(today_str)
    if sent_at is None:
        return  # no pending cap alert today

    elapsed_minutes = (datetime.now(IST) - sent_at).total_seconds() / 60
    if elapsed_minutes < 50:
        return  # still within the grace window

    reason = f"No response to the cap-hit alert within 50 minutes (sent {sent_at.strftime('%I:%M %p')} IST)"
    status = {
        "paused": True,
        "reason": reason,
        "updated_at": datetime.now(IST).strftime("%Y-%m-%d %I:%M %p IST"),
        "auto_pause_date": today_str,
    }
    PIPELINE_STATUS_FILE.write_text(json.dumps(status, indent=2))
    log.info("Auto-paused for the rest of today due to cap alert timeout.")
    send_slack_message_with_control_buttons(
        f":alarm_clock: *Swiggy lead sync auto-paused for today* — no response to the cap-increase "
        f"alert within 50 minutes.\nRemaining leads today are staying in the archive as *capped* "
        f"(not lost) — use the 'Repush Capped Leads' workflow to send them once you're ready. "
        f"This resumes automatically tomorrow, or hit Start Sync to resume right now."
    )


def main():
    # Cap removed: no cap-timeout auto-pause, no capped status, no cap alert.
    pause_status = is_pipeline_paused()
    is_paused = pause_status["paused"]

    # Scheduled runs (from the Worker) post at most ONE message per run,
    # at the end, and only when there's something worth saying - see the
    # decision block at the bottom of main(). Manual runs keep the old
    # behaviour (start message + summary every time).
    is_scheduled = RUN_SLOT in ("first", "followup")

    today_str_for_start_msg = datetime.now(IST).strftime("%Y-%m-%d")
    now_str = datetime.now(IST).strftime("%I:%M %p")
    if is_paused:
        reason_part = f" Reason: {pause_status['reason']}" if pause_status["reason"] else ""
        paused_since = f" (paused since {pause_status['updated_at']})" if pause_status["updated_at"] else ""
        log.info("Pipeline is PAUSED%s - will still fetch and archive leads, but NOT push to the API.%s", paused_since, reason_part)
    if is_scheduled:
        log.info("Scheduled run (slot=%s) - start message deferred until we know there are new leads.", RUN_SLOT)
    elif is_paused:
        send_slack_alert(
            f":arrows_counterclockwise: *Swiggy lead sync started (PAUSED)* ({today_str_for_start_msg} {now_str} IST)\n"
            f"No leads will be pushed this run{reason_part} - but any new leads found will still be "
            f"archived with status *not_pushed*, so nothing is lost or hidden while paused."
        )
    else:
        send_slack_alert(f":arrows_counterclockwise: *Swiggy lead sync started* ({today_str_for_start_msg} {now_str} IST)")

    rows, newest_file_mtime = fetch_csv_from_sftp()

    today_str_check = datetime.now(IST).strftime("%Y-%m-%d")
    stale_file_note = ""
    if newest_file_mtime is None:
        log.warning("Could not determine the data file's last-modified time.")
    elif newest_file_mtime.strftime("%Y-%m-%d") != today_str_check:
        log.warning(
            "The newest file on SFTP was last modified on %s, not today (%s) - "
            "likely no fresh export has landed yet. Any rows in it are probably "
            "already-pushed leads (safely skipped by dedup), not new data.",
            newest_file_mtime.strftime("%Y-%m-%d %H:%M IST"), today_str_check
        )
        stale_file_note = (
            f"\n:warning: Newest file on SFTP is from *{newest_file_mtime.strftime('%Y-%m-%d %H:%M')} IST* "
            f"- the upstream export may not be running."
        )
        if not is_scheduled:
            send_slack_alert(
                f":warning: *Swiggy lead sync: no fresh file today.* The newest file on "
                f"SFTP (checked both directories) is from *{newest_file_mtime.strftime('%Y-%m-%d %H:%M')} IST* — "
                f"proceeding anyway (dedup will skip anything already pushed), but if this "
                f"keeps showing up, the upstream export may not be running."
            )
    else:
        log.info("Newest data file is from today (%s) - looks fresh.", newest_file_mtime.strftime("%H:%M IST"))

    seen_ids = load_seen_ids()
    previously_failed_ids = load_previously_failed_ids() - seen_ids
    not_pushed_logged_ids = load_already_logged_not_pushed_ids() if is_paused else set()
    daily_state = load_daily_state()
    today_str = daily_state["date"]

    # Scheduled runs: post "sync started" only when this run actually has
    # new leads to act on - so a run that finds only duplicates stays quiet.
    if is_scheduled:
        new_ids = {
            (r.get("BOOKING_ORDER_ID") or "").strip() for r in rows
        } - seen_ids - not_pushed_logged_ids - previously_failed_ids - {""}
        if new_ids:
            if is_paused:
                send_slack_alert(
                    f":arrows_counterclockwise: *Swiggy lead sync started (PAUSED)* ({today_str_for_start_msg} {now_str} IST)\n"
                    f"*{len(new_ids)}* new lead(s) found - archiving as *not_pushed*, nothing sent to SquadStack."
                )
            else:
                send_slack_alert(
                    f":arrows_counterclockwise: *Swiggy lead sync started* ({today_str_for_start_msg} {now_str} IST)\n"
                    f"*{len(new_ids)}* new lead(s) found - pushing now."
                )

    # Every BOOKING_ORDER_ID handled in THIS run, whatever the outcome.
    # Catches a booking that appears in both the old and new directory
    # (or twice in one export): it's processed once, not pushed twice,
    # and a failure isn't retried and double-logged in the same run.
    handled_this_run = set()

    total, pushed, skipped, failed, not_pushed_count = 0, 0, 0, 0, 0
    retry_failed = 0  # leads that failed in an earlier run and failed again now

    with requests.Session() as session:
        for raw_row in rows:
            total += 1
            booking_id = (raw_row.get("BOOKING_ORDER_ID") or "").strip()

            if not booking_id:
                log.warning("Row %s missing BOOKING_ORDER_ID, skipping", total)
                skipped += 1
                continue

            if booking_id in seen_ids or booking_id in handled_this_run:
                skipped += 1
                continue
            handled_this_run.add(booking_id)

            enriched_row = transform_row(raw_row)
            payload = build_payload(enriched_row)

            # PAUSED: never call the API. Archive with status "not_pushed"
            # so the lead's data is visible and preserved, but only once
            # per lead - not_pushed_logged_ids avoids re-writing the same
            # lead every single run for as long as the pause lasts. Once
            # resumed, these leads aren't in seen_ids yet, so they'll be
            # picked up and actually pushed normally on the next run.
            if is_paused:
                if booking_id in not_pushed_logged_ids:
                    skipped += 1
                    continue
                not_pushed_count += 1
                archive_lead_row(today_str, payload, status="not_pushed")
                not_pushed_logged_ids.add(booking_id)
                continue

            success, error_detail = push_lead(payload, session)
            if success:
                pushed += 1
                append_seen_id(booking_id)
                seen_ids.add(booking_id)
                daily_state["count"] += 1
                save_daily_state(daily_state)
                archive_lead_row(today_str, payload, status="pushed")
            else:
                if booking_id in previously_failed_ids:
                    # Already reported when it first failed - retry quietly.
                    retry_failed += 1
                    log.info("Lead lead_id=%s failed again (already reported earlier).", booking_id)
                    continue
                failed += 1
                archive_lead_row(today_str, payload, status="failed")
                append_failed_lead(today_str, payload, error_detail)

    update_daily_summary(
        today_str, new_leads=total - skipped - retry_failed, pushed=pushed, capped=0,
        failed=failed, not_pushed=not_pushed_count
    )

    log.info(
        "Run complete. paused=%s total=%s pushed=%s skipped(already seen/logged)=%s failed=%s "
        "not_pushed(paused)=%s still_failing_from_earlier=%s daily_count=%s",
        is_paused, total, pushed, skipped, failed, not_pushed_count, retry_failed, daily_state["count"]
    )

    # ---- Decide whether this run posts to Slack -------------------------
    # new_this_run = leads this run actually acted on (pushed, failed or
    # archived as not_pushed). Pure duplicates don't count.
    new_this_run = pushed + failed + not_pushed_count
    batch_hour = current_batch_hour()
    hour_state = load_hour_state(batch_hour)
    hour_state["new_leads"] = int(hour_state.get("new_leads", 0)) + new_this_run

    post_summary = True
    if is_scheduled and new_this_run == 0:
        post_summary = False
        if RUN_SLOT == "first":
            log.info("First run of batch %s found nothing new - staying silent.", batch_hour)
        elif hour_state["new_leads"] == 0 and not hour_state.get("quiet_notified"):
            # Follow-up run, and NO run this hour has found anything new:
            # post the single "nothing new" message for this batch.
            hour_state["quiet_notified"] = True
            send_slack_message_with_control_buttons(
                f":zzz: *Swiggy lead sync: no new leads this hour* ({today_str} IST, checked {batch_hour[-2:]}:45 and "
                f"{datetime.now(IST).strftime('%H:%M')})\n"
                f"Only duplicates found ({skipped} already pushed) - nothing new pushed. "
                f"Today's total pushed: {daily_state['count']}{stale_file_note}"
            )
        else:
            log.info("Follow-up run of batch %s found nothing new - this hour already reported, staying silent.", batch_hour)

    save_hour_state(hour_state)

    if not post_summary:
        return

    if is_paused:
        summary_lines = [
            f":no_entry: *Swiggy lead sync run complete (PAUSED)* ({today_str} IST)",
            f"New leads found: *{not_pushed_count}* (archived as *not_pushed*, not sent to SquadStack)  |  Already logged: {skipped}",
        ]
    else:
        status_emoji = ":white_check_mark:" if failed == 0 else ":warning:"
        summary_lines = [
            f"{status_emoji} *Swiggy lead sync run complete* ({today_str} IST)",
            f"Pushed: *{pushed}*  |  Failed: *{failed}*  |  Skipped (dupes): {skipped}",
            f"Today's total pushed: {daily_state['count']}",
        ]
        if failed:
            summary_lines.append(f":x: {failed} lead(s) failed to push this run - see failed_leads.csv in the private repo.")
        if retry_failed:
            summary_lines.append(f"({retry_failed} lead(s) from earlier runs are still failing - already reported.)")
    send_slack_message_with_control_buttons("\n".join(summary_lines))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("Sync run crashed")
        send_slack_alert(
            f":red_circle: *Swiggy lead sync CRASHED* before completing "
            f"({datetime.now(IST).strftime('%Y-%m-%d %I:%M %p')} IST)\n"
            f"Error: `{type(e).__name__}: {e}`\nCheck the GitHub Actions log for the full traceback."
        )
        raise
