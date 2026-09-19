#!/usr/bin/env python3
"""Read-only standalone probe for the CLI-metadata lookup / sidebar latency.

Measures, against the ACTIVE profile's state.db (same resolution the WebUI
server uses):

  0. the interactive pass cold/warm
     (``read_importable_agent_session_rows(limit=20, exclude_sources=("cron",
     "webhook", "kanban"))`` — the sidebar's visible CLI/agent window, and the
     Slice D candidate-ordering target). Measured FIRST in the process so the
     first sample is the process's first state.db read (cold); later samples
     are warm.
  1. the Claude Code JSONL scan alone (``get_claude_code_sessions()``),
  2. the OLD lookup cost: full ``get_cli_sessions()`` projection + a linear
     scan for the sid (exactly what routes.py did before Slice A),
  3. the NEW targeted lookup: ``models.lookup_cli_session_metadata(sid)``.

SAFETY: this script never writes to the store. It opens state.db read-only
(URI mode=ro), and it monkeypatches every helper that could otherwise create
state: ``ensure_cron_project`` / ``ensure_webhook_project`` (projects.json),
``_profile_has_user_projects``, and ``get_last_workspace`` (workspace probes).
If ``idx_messages_session`` is missing from the target db, the projection's
defensive index self-heal would open a writable connection — the probe checks
for that up front and refuses to run unless ``--allow-index-selfheal`` is
passed (the live dbs all have the index).

Usage (from the worktree root):
    python scripts/probe_sidebar_latency.py [--sid SID] [--runs 3]
"""

from __future__ import annotations

import argparse
import pathlib
import sqlite3
import statistics
import sys
import time
from contextlib import closing

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _pick_newest_interactive_sid(db_path: pathlib.Path) -> str | None:
    """Newest non-background sid via one indexed read (not part of the timing)."""
    try:
        conn = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    with closing(conn):
        cur = conn.cursor()
        for order in (
            "COALESCE(s.last_activity_at, s.started_at) DESC, s.started_at DESC",
            "s.started_at DESC",
        ):
            try:
                cur.execute(
                    "SELECT s.id FROM sessions s WHERE s.source IS NOT NULL"
                    " AND s.source NOT IN ('cron','webhook','kanban')"
                    f" ORDER BY {order} LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    return str(row[0])
            except sqlite3.Error:
                continue
    return None


def _messages_index_present(db_path: pathlib.Path) -> bool:
    try:
        conn = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True)
    except sqlite3.Error:
        return False
    with closing(conn):
        try:
            cur = conn.cursor()
            cur.execute("PRAGMA index_list(messages)")
            return any(str(row[1]) == "idx_messages_session" for row in cur.fetchall())
        except sqlite3.Error:
            return False


def _timed(fn, runs: int) -> tuple[list[float], object]:
    samples: list[float] = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = fn()
        samples.append((time.perf_counter() - start) * 1000.0)
    return samples, result


def _fmt(samples: list[float]) -> str:
    return (
        f"median {statistics.median(samples):8.1f} ms | "
        f"min {min(samples):8.1f} | max {max(samples):8.1f} | "
        f"n={len(samples)}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sid", default=None, help="session id to look up")
    parser.add_argument("--runs", type=int, default=3, help="samples per measurement")
    parser.add_argument(
        "--allow-index-selfheal",
        action="store_true",
        help="run even if idx_messages_session is missing (the projection may then "
        "open a writable connection to self-heal it)",
    )
    args = parser.parse_args()

    import api.models as models

    # Read-only safety: never let the probe mint projects or touch workspace state.
    models.ensure_cron_project = lambda **_: None
    models.ensure_webhook_project = lambda: None
    models._profile_has_user_projects = lambda: False
    models.get_last_workspace = lambda: pathlib.Path("/tmp/probe-workspace")

    hermes_home, db_path, cli_profile, _cache_key = models._resolve_cli_sessions_context(None)
    print(f"hermes_home : {hermes_home}")
    print(f"state.db    : {db_path}  (profile={cli_profile or 'default'})")

    if not pathlib.Path(db_path).exists():
        print("state.db missing — nothing to measure.")
        return 2

    if not _messages_index_present(pathlib.Path(db_path)) and not args.allow_index_selfheal:
        print(
            "refusing to run: idx_messages_session is missing, so the projection "
            "would self-heal with a writable connection. Re-run with "
            "--allow-index-selfheal to accept that (never on the live store)."
        )
        return 3

    runs = max(1, args.runs)

    # 0) Interactive pass (the sidebar's visible CLI/agent window). Measured
    # FIRST in the process so sample 1 is the process's first state.db read
    # (cold); later samples are warm. Pre-Slice-D this pass ordered its
    # candidate window with a correlated per-row MAX(messages.timestamp)
    # subquery; after the swap it uses the indexed
    # COALESCE(s.last_activity_at, s.started_at) key.
    def interactive_pass():
        return models.read_importable_agent_session_rows(
            db_path,
            limit=models.CLI_VISIBLE_SESSION_LIMIT,
            exclude_sources=("cron", "webhook", "kanban"),
        )

    cold_ms = None
    warm_samples: list[float] = []
    interactive_rows = 0
    for sample_index in range(runs):
        start = time.perf_counter()
        rows = interactive_pass()
        elapsed = (time.perf_counter() - start) * 1000.0
        interactive_rows = len(rows or [])
        if sample_index == 0:
            cold_ms = elapsed
        else:
            warm_samples.append(elapsed)
    warm_text = (
        f"median {statistics.median(warm_samples):8.1f} ms | "
        f"min {min(warm_samples):8.1f} | max {max(warm_samples):8.1f} | n={len(warm_samples)}"
        if warm_samples
        else "n/a (runs=1)"
    )
    print(
        f"interactive pass (limit={models.CLI_VISIBLE_SESSION_LIMIT}): "
        f"cold {cold_ms:8.1f} ms | warm {warm_text}  rows={interactive_rows}"
    )
    print()

    sid = args.sid or _pick_newest_interactive_sid(pathlib.Path(db_path))
    if not sid:
        print("no candidate session id found — pass --sid.")
        return 2
    print(f"target sid  : {sid}")
    print()

    cc_samples, cc_rows = _timed(lambda: models.get_claude_code_sessions(), runs)
    print(f"claude_code JSONL scan      : {_fmt(cc_samples)}  rows={len(cc_rows or [])}")

    def old_lookup():
        models.clear_cli_sessions_cache()  # cold, like the pre-Slice-A per-lookup rebuild
        for row in models.get_cli_sessions(all_profiles=False):
            if row.get("session_id") == sid:
                return row
        return {}

    before_samples, before_row = _timed(old_lookup, runs)
    print(f"OLD lookup (cold projection): {_fmt(before_samples)}  hit={bool(before_row)}")

    def warm_old_lookup():
        return models.get_cli_sessions(all_profiles=False)

    warm_samples, _ = _timed(warm_old_lookup, runs)
    print(f"OLD bulk projection (warm)  : {_fmt(warm_samples)}")

    after_samples, after_row = _timed(
        lambda: models.lookup_cli_session_metadata(sid), runs
    )
    print(f"NEW targeted lookup         : {_fmt(after_samples)}  hit={bool(after_row)}")
    print()

    if before_row and after_row and before_row != after_row:
        print("WARNING: lookup row differs from the bulk row for this sid:")
        keys = sorted(set(before_row) | set(after_row))
        for key in keys:
            if before_row.get(key) != after_row.get(key):
                print(f"  {key}: bulk={before_row.get(key)!r} lookup={after_row.get(key)!r}")
    elif not before_row and after_row:
        print(
            "note: the bulk window did NOT contain this sid (capped at "
            f"{models.CLI_VISIBLE_SESSION_LIMIT} rows); the targeted lookup resolves it "
            "— fidelity improvement over the old linear scan."
        )
    elif before_row and not after_row:
        print("WARNING: bulk contained this sid but the targeted lookup missed it.")
    else:
        print("equivalence: lookup row == bulk row (or both empty).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
