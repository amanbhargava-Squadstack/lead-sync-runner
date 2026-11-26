#!/usr/bin/env python3
"""
Reports current pipeline status to Slack on demand - whether the sync
is paused (and why), today's effective cap, and today's actual pushed
count. Doesn't touch SFTP or the SquadStack API; just reads the
already-committed archive files.
"""

from datetime import datetime

import swiggy_lead_sync as sync


def main():
    today_str = datetime.now(sync.IST).strftime("%Y-%m-%d")

    pause_status = sync.is_pipeline_paused()
    actual_pushed = sync.get_archive_pushed_count(today_str)

    if pause_status["paused"]:
        reason_part = f"\nReason: {pause_status['reason']}" if pause_status["reason"] else ""
        since_part = f"\nSince: {pause_status['updated_at']}" if pause_status["updated_at"] else ""
        status_line = f":no_entry: *PAUSED*{since_part}{reason_part}"
    else:
        status_line = ":large_green_circle: *RUNNING* (not paused)"

    lines = [
        f":mag: *Swiggy lead sync status check* ({today_str} IST)",
        status_line,
        f"Today's total pushed: *{actual_pushed}* (no daily cap)",
    ]

    sync.send_slack_message_with_control_buttons("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
