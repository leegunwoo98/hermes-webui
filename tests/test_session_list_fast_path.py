"""Slice C — fast first-paint sidebar payload: parity + cache wiring.

The fast payload (``api.routes._build_session_list_fast_payload``) is the cold
first-paint path for the default sidebar request shape. It must render the same
visible list as the full builder (``_build_session_list_cache_payload``) for the
same args, while never paying the unbounded pipeline costs: no messages JOIN
aggregates over the whole candidate window, no Claude Code JSONL scan, no
orphan-prune probes.

Parity scope (plan v2 acceptance criterion 3): ids/order/title/updated_at/
message_count/source flags/project_id/pinned/archived/relationship_type/
parent_session_id, plus every payload count field.
"""
import json
import sqlite3
import threading
import time
from collections import OrderedDict
from pathlib import Path

import pytest

import api.config as config
import api.models as models
import api.routes as routes

pytestmark = pytest.mark.requires_agent_modules


# ── fixture ──────────────────────────────────────────────────────────────────

T = 1_700_000_000.0

_SESSION_COLUMNS = (
    "id", "source", "title", "model", "started_at", "message_count",
    "last_activity_at", "parent_session_id", "end_reason", "ended_at",
    "session_source", "user_id", "chat_id", "chat_type", "thread_id",
    "session_key", "origin_chat_id", "origin_user_id", "platform",
)

# (id, source, title, started_at, end_reason, ended_at, parent, message_count,
#  extra_columns, messages[(role, ts)])
_FIXTURE_SESSIONS = [
    # compression chain: root collapsed into the tip
    ("comp-root", "cli", "Compression root", T + 100, "compression", T + 200, None, 4, {}, [
        ("user", T + 110), ("assistant", T + 120), ("user", T + 190), ("assistant", T + 200)]),
    ("comp-tip", "cli", "Compression tip", T + 210, None, None, "comp-root", 3, {}, [
        ("user", T + 215), ("assistant", T + 230), ("user", T + 240)]),
    # cli_close chain: same continuation collapse
    ("close-root", "cli", "Close root", T + 300, "cli_close", T + 400, None, 2, {}, [
        ("user", T + 310), ("assistant", T + 400)]),
    ("close-tip", "cli", "Close tip", T + 410, None, None, "close-root", 2, {}, [
        ("user", T + 415), ("assistant", T + 430)]),
    # plain child: parent ended for another reason -> real child_session row
    ("plain-parent", "cli", "Plain parent", T + 500, "agent_close", T + 560, None, 3, {}, [
        ("user", T + 505), ("assistant", T + 510), ("user", T + 560)]),
    ("plain-child", "cli", "Plain child", T + 520, None, None, "plain-parent", 2, {}, [
        ("user", T + 530), ("assistant", T + 600)]),
    # messaging identity pair: newest wins under the per-source dedupe
    ("slack-old", "slack", "Slack thread old", T + 700, None, None, None, 2,
     {"chat_id": "C123", "user_id": "U9", "session_key": "slack:C123", "platform": "slack"}, [
        ("user", T + 705), ("assistant", T + 710)]),
    ("slack-new", "slack", "Slack thread new", T + 720, None, None, None, 2,
     {"chat_id": "C123", "user_id": "U9", "session_key": "slack:C123", "platform": "slack"}, [
        ("user", T + 725), ("assistant", T + 730)]),
    # subagent parent + child (the reader re-adds the parent when the child wins)
    ("sub-parent", "subagent", "Subagent orchestrator", T + 800, None, None, None, 2, {}, [
        ("user", T + 805), ("assistant", T + 810)]),
    ("sub-child", "subagent", "Subagent leaf", T + 900, None, None, "sub-parent", 3, {}, [
        ("user", T + 905), ("assistant", T + 910), ("user", T + 1000)]),
    # zero-message row: excluded by the fast SQL gate and dropped by the projection
    ("zero-msg", "cli", "Empty CLI", T + 50, None, None, None, 0, {}, []),
    # webui-source state.db row (mirrors a WebUI sidecar; must dedupe, not double-render)
    ("webui-state-row", "webui", "WebUI native", T + 1100, None, None, None, 3, {}, [
        ("user", T + 1105), ("assistant", T + 1110), ("user", T + 1115)]),
    # background chip rows
    ("cron_job1_20260101_000000", "cron", "Cron run", T + 950, None, None, None, 2, {}, [
        ("user", T + 955), ("assistant", T + 960)]),
    ("kanban-card-1", "kanban", "Kanban card", T + 960, None, None, None, 2, {}, [
        ("user", T + 965), ("assistant", T + 970)]),
    # default-titled CLI row with user turns: visible only when user counts are known
    ("cli-untitled", "cli", None, T + 1200, None, None, None, 2, {}, [
        ("user", T + 1205), ("assistant", T + 1210)]),
    # ACP row: visible only when its user turns are known
    ("acp-row", "acp", None, T + 1300, None, None, None, 2, {}, [
        ("user", T + 1305), ("assistant", T + 1310)]),
    # state.db claude-code row: reachable from the first pass, no JSONL scan
    ("claude-code-db-row", "claude-code", "Claude Code import", T + 1400, None, None, None, 2, {}, [
        ("user", T + 1405), ("assistant", T + 1410)]),
    # stale-counter branch: an EMPTY tip whose column lies (mc=3, no messages)
    # must not steal the compression-tip selection from the real tip
    ("stale-root", "cli", "Stale counter root", T + 1900, "compression", T + 1950, None, 2, {}, [
        ("user", T + 1905), ("assistant", T + 1950)]),
    ("stale-fresh-tip", "cli", "Stale counter fresh tip", T + 1960, None, None, "stale-root", 2, {}, [
        ("user", T + 1965), ("assistant", T + 1990)]),
    ("stale-empty-tip", "cli", "Stale counter empty tip", T + 2000, None, None, "stale-root", 3, {}, []),
    # zero-column row WITH persisted messages (mc=0, one user turn): the count
    # must fall back to the messages table, not hide the row
    ("cli-zero-col-active", "cli", None, T + 2100, None, None, None, 0, {}, [
        ("user", T + 2105)]),
]

# sidecars: (session_id, title, messages, extra)
_FIXTURE_SIDECARS = [
    ("webui-state-row", "WebUI native", [
        {"role": "user", "content": "webui turn", "timestamp": T + 1105},
        {"role": "assistant", "content": "webui answer", "timestamp": T + 1110},
        {"role": "user", "content": "webui follow", "timestamp": T + 1115},
    ], {}),
    ("webui-pure", "Pure WebUI session", [
        {"role": "user", "content": "pure webui", "timestamp": T + 1500},
        {"role": "assistant", "content": "answer", "timestamp": T + 1505},
    ], {}),
    ("arch-parent", "Archived parent", [
        {"role": "user", "content": "archived parent", "timestamp": T + 1600},
        {"role": "assistant", "content": "answer", "timestamp": T + 1605},
    ], {"archived": True}),
    ("arch-child", "Visible child of archived parent", [
        {"role": "user", "content": "child", "timestamp": T + 1700},
        {"role": "assistant", "content": "answer", "timestamp": T + 1705},
    ], {"parent_session_id": "arch-parent"}),
    ("other-profile-row", "Other profile session", [
        {"role": "user", "content": "other profile", "timestamp": T + 1800},
        {"role": "assistant", "content": "answer", "timestamp": T + 1805},
    ], {"profile": "work"}),
]


def _make_state_db(path: Path, sessions, *, last_activity_lag=None, last_activity_null=()):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE sessions (%s)" % ", ".join(
            "id TEXT PRIMARY KEY" if col == "id" else f"{col} "
            + ("INTEGER" if col == "message_count" else "REAL" if col in ("started_at", "last_activity_at", "ended_at") else "TEXT")
            for col in _SESSION_COLUMNS
        )
    )
    conn.execute(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT, "
        "role TEXT, content TEXT, timestamp REAL)"
    )
    conn.execute(
        "CREATE INDEX idx_sessions_effective_activity ON sessions("
        "COALESCE(last_activity_at, started_at) DESC, started_at DESC)"
    )
    conn.execute("CREATE INDEX idx_messages_session ON messages(session_id, timestamp)")
    for sid, source, title, started, end_reason, ended, parent, mc, extra, messages in sessions:
        last_message = max((ts for _role, ts in messages), default=None)
        lag = (last_activity_lag or {}).get(sid, 0)
        last_activity = None if sid in last_activity_null else (
            (last_message if last_message is not None else started) - lag
        )
        row = {
            "id": sid, "source": source, "title": title, "model": "test-model",
            "started_at": started, "message_count": mc, "last_activity_at": last_activity,
            "parent_session_id": parent, "end_reason": end_reason, "ended_at": ended,
            **extra,
        }
        conn.execute(
            "INSERT INTO sessions (%s) VALUES (%s)" % (
                ", ".join(_SESSION_COLUMNS), ", ".join("?" for _ in _SESSION_COLUMNS)),
            tuple(row.get(col) for col in _SESSION_COLUMNS),
        )
        for role, ts in messages:
            conn.execute(
                "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
                (sid, role, f"{sid} {role}", ts),
            )
    conn.commit()
    conn.close()


def _install_fixture(monkeypatch, tmp_path, *, last_activity_lag=None, last_activity_null=()):
    import api.profiles as profiles

    state_dir = tmp_path / "webui"
    session_dir = state_dir / "sessions"
    session_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(config, "STATE_DIR", state_dir, raising=False)
    monkeypatch.setattr(config, "SESSION_DIR", session_dir, raising=False)
    monkeypatch.setattr(config, "SESSION_INDEX_FILE", session_dir / "_index.json", raising=False)
    monkeypatch.setattr(config, "SETTINGS_FILE", state_dir / "settings.json", raising=False)
    monkeypatch.setattr(models, "SESSION_DIR", session_dir, raising=False)
    monkeypatch.setattr(models, "SESSION_INDEX_FILE", session_dir / "_index.json", raising=False)
    monkeypatch.setattr(models, "SESSIONS", OrderedDict(), raising=False)
    monkeypatch.setattr(profiles, "get_active_hermes_home", lambda: tmp_path, raising=False)
    monkeypatch.setattr(profiles, "get_active_profile_name", lambda: "default", raising=False)
    monkeypatch.setattr(models, "_active_state_db_path", lambda: tmp_path / "state.db", raising=False)
    monkeypatch.setattr(routes, "_active_state_db_path", lambda: tmp_path / "state.db", raising=False)
    # Never mint projects / touch workspace state from a fixture build.
    monkeypatch.setattr(models, "ensure_cron_project", lambda **_kw: "cron-project", raising=False)
    monkeypatch.setattr(models, "ensure_webhook_project", lambda: "webhook-project", raising=False)
    monkeypatch.setattr(models, "_profile_has_user_projects", lambda: True, raising=False)
    monkeypatch.setattr(models, "get_last_workspace", lambda: "/tmp/fixture-workspace", raising=False)

    (state_dir / "settings.json").write_text(json.dumps({
        "show_cli_sessions": True,
        "show_previous_messaging_sessions": False,
        "show_cron_sessions": False,
        "show_claude_code_sessions": True,
        "show_webhook_sessions": False,
        "show_kanban_sessions": False,
        "api_redact_enabled": False,
    }), encoding="utf-8")

    _make_state_db(
        tmp_path / "state.db", _FIXTURE_SESSIONS,
        last_activity_lag=last_activity_lag, last_activity_null=last_activity_null,
    )

    index_entries = []
    for sid, title, messages, extra in _FIXTURE_SIDECARS:
        sidecar = {
            "session_id": sid,
            "title": title,
            "workspace": "/tmp/fixture-workspace",
            "model": "test-model",
            "messages": messages,
            "created_at": messages[0]["timestamp"],
            "updated_at": messages[-1]["timestamp"],
            "last_message_at": messages[-1]["timestamp"],
            "message_count": len(messages),
            "profile": extra.get("profile", "default"),
        }
        if extra.get("archived"):
            sidecar["archived"] = True
        if extra.get("parent_session_id"):
            sidecar["parent_session_id"] = extra["parent_session_id"]
        (session_dir / f"{sid}.json").write_text(json.dumps(sidecar), encoding="utf-8")
        index_entries.append(dict(sidecar))
    (session_dir / "_index.json").write_text(json.dumps(index_entries), encoding="utf-8")

    models.clear_sidecar_metadata_cache()
    models.clear_cli_sessions_cache()
    return state_dir


def _payload_args(**overrides):
    args = dict(
        active_profile="default",
        all_profiles=False,
        show_cli_sessions=True,
        show_previous_messaging_sessions=False,
        show_cron_sessions=False,
        show_claude_code_sessions=True,
        include_archived=False,
        exclude_hidden=False,
        visible_only=True,
        show_webhook_sessions=False,
        show_kanban_sessions=False,
        source_filter=None,
        sidebar_source=None,
        archived_limit=None,
        archived_offset=0,
    )
    args.update(overrides)
    return args


def _build_full(**overrides):
    return routes._build_session_list_cache_payload(**_payload_args(**overrides))


def _build_fast(**overrides):
    return routes._build_session_list_fast_payload(**_payload_args(**overrides))


_ROW_PARITY_FIELDS = (
    "session_id", "title", "display_title", "_state_db_title",
    "updated_at", "last_message_at", "message_count", "actual_message_count",
    "is_cli_session", "source_tag", "raw_source", "session_source", "source_label",
    "project_id", "pinned", "archived", "read_only", "profile",
    "relationship_type", "parent_session_id", "parent_title", "parent_source",
    "_parent_lineage_root_id", "_lineage_root_id", "_lineage_tip_id",
    "_compression_segment_count",
)

_COUNT_FIELDS = (
    "cli_count", "archived_count", "archived_webui_count", "archived_cli_count",
    "webui_session_count", "cli_session_count", "other_profile_count",
    "all_profiles", "active_profile",
)


def _assert_payload_parity(full, fast):
    assert [r["session_id"] for r in fast["sessions"]] == [r["session_id"] for r in full["sessions"]]
    full_by_id = {r["session_id"]: r for r in full["sessions"]}
    for row in fast["sessions"]:
        ref = full_by_id[row["session_id"]]
        for field in _ROW_PARITY_FIELDS:
            assert row.get(field) == ref.get(field), (
                f"{row['session_id']}: {field}: fast={row.get(field)!r} full={ref.get(field)!r}"
            )
    assert [r["session_id"] for r in fast["sidebar_reference_sessions"]] == [
        r["session_id"] for r in full["sidebar_reference_sessions"]
    ]
    for field in _COUNT_FIELDS:
        assert fast.get(field) == full.get(field), f"{field}: fast={fast.get(field)!r} full={full.get(field)!r}"


# ── C3: parity ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("shape", [
    {},
    {"exclude_hidden": True},
    {"sidebar_source": "webui"},
    {"sidebar_source": "cli"},
    {"show_cli_sessions": False},
    {"show_previous_messaging_sessions": True},
    {"show_cron_sessions": True},
    {"show_kanban_sessions": True},
])
def test_fast_payload_parity_with_full_builder(monkeypatch, tmp_path, shape):
    _install_fixture(monkeypatch, tmp_path)
    full = _build_full(**shape)
    fast = _build_fast(**shape)
    _assert_payload_parity(full, fast)


def test_fast_payload_parity_under_candidate_key_drift(monkeypatch, tmp_path):
    """A lagging/NULL ``last_activity_at`` must not change the visible slice."""
    _install_fixture(
        monkeypatch, tmp_path,
        last_activity_lag={"plain-child": 30.0, "sub-child": 25.0},
        last_activity_null=("slack-new",),
    )
    full = _build_full()
    fast = _build_fast()
    _assert_payload_parity(full, fast)
    ids = [r["session_id"] for r in fast["sessions"]]
    # A collapsed chain renders as its TIP id (navigation points at the latest
    # importable segment) carrying the root's lineage identity.
    assert "comp-tip" in ids and "comp-root" not in ids
    assert "close-tip" in ids and "close-root" not in ids
    merged = next(r for r in fast["sessions"] if r["session_id"] == "comp-tip")
    assert merged["_lineage_root_id"] == "comp-root"
    assert merged["_lineage_tip_id"] == "comp-tip"
    assert merged["_compression_segment_count"] == 2
    assert merged["title"] == "Compression root"
    assert "plain-child" in ids  # real child row survives
    child = next(r for r in fast["sessions"] if r["session_id"] == "plain-child")
    assert child["relationship_type"] == "child_session"
    assert child["parent_session_id"] == "plain-parent"
    assert child["parent_title"] == "Plain parent"
    assert "slack-new" in ids and "slack-old" not in ids  # messaging dedupe
    assert "zero-msg" not in ids
    # stale-counter branch: the empty lying tip must not steal the collapse
    assert "stale-fresh-tip" in ids
    assert "stale-empty-tip" not in ids
    assert "stale-root" not in ids
    stale = next(r for r in fast["sessions"] if r["session_id"] == "stale-fresh-tip")
    assert stale["title"] == "Stale counter root"
    assert stale["_lineage_root_id"] == "stale-root"
    # mc=0 with a persisted message must stay visible (count from the table)
    assert "cli-zero-col-active" in ids
    zero_col = next(r for r in fast["sessions"] if r["session_id"] == "cli-zero-col-active")
    assert zero_col["message_count"] == 1
    assert "sub-parent" in ids and "sub-child" in ids  # subagent re-add
    assert "webui-state-row" in ids
    assert "webui-pure" in ids
    assert "arch-parent" not in ids
    assert "other-profile-row" not in ids
    assert any(
        r["session_id"] == "arch-parent" for r in fast["sidebar_reference_sessions"]
    )


def test_fast_payload_keeps_untitled_cli_and_acp_rows_visible(monkeypatch, tmp_path):
    """User-turn counts are unknown in the fast window; the bounded fallback
    query must still keep rows the full pipeline keeps."""
    _install_fixture(monkeypatch, tmp_path)
    fast = _build_fast()
    ids = {r["session_id"] for r in fast["sessions"]}
    assert "cli-untitled" in ids
    assert "acp-row" in ids
    full = _build_full()
    assert {r["session_id"] for r in full["sessions"]} == ids


def test_fast_payload_never_runs_claude_code_jsonl_scan(monkeypatch, tmp_path):
    _install_fixture(monkeypatch, tmp_path)

    def _boom():
        raise AssertionError("Claude Code JSONL scan must never run on the fast path")

    monkeypatch.setattr(models, "get_claude_code_sessions", _boom)
    monkeypatch.setattr(routes, "get_claude_code_sessions", _boom, raising=False)
    fast = _build_fast()
    ids = {r["session_id"] for r in fast["sessions"]}
    assert "claude-code-db-row" in ids  # state.db claude-code rows stay reachable
    assert not any(str(sid).startswith("claude_code_") for sid in ids)


def _record_sqlite_connect(monkeypatch):
    """Record every executed statement while delegating to the real sqlite3."""
    statements: list[str] = []
    real_connect = sqlite3.connect

    class _RecordingCursor:
        def __init__(self, cursor):
            self._cursor = cursor

        def execute(self, sql, *args):
            statements.append(" ".join(str(sql).split()))
            return self._cursor.execute(sql, *args)

        def __getattr__(self, name):
            return getattr(self._cursor, name)

    class _RecordingConnection:
        def __init__(self, conn):
            object.__setattr__(self, "_conn", conn)

        def __setattr__(self, name, value):
            # row_factory is set on the connection by the readers; forward it to
            # the real connection instead of shadowing it on the wrapper.
            setattr(self._conn, name, value)

        def __getattr__(self, name):
            return getattr(self._conn, name)

        def cursor(self):
            return _RecordingCursor(self._conn.cursor())

    monkeypatch.setattr(
        sqlite3, "connect",
        lambda *args, **kwargs: _RecordingConnection(real_connect(*args, **kwargs)),
    )
    return statements


def test_fast_payload_window_keeps_exact_counts_and_skips_user_turn_aggregation(monkeypatch, tmp_path):
    """The fast window keeps exact per-candidate counts/recency but must not
    aggregate user turns for the whole candidate set (deferred to a bounded
    fallback)."""
    _install_fixture(monkeypatch, tmp_path)
    statements = _record_sqlite_connect(monkeypatch)
    fast = _build_fast()
    assert fast["sessions"]

    fast_sql = [s for s in statements if "FROM sessions s" in s and "candidates" in s.lower()]
    assert fast_sql, "the fast window query must run"
    for sql in fast_sql:
        assert "LOWER(m.role)" not in sql, f"fast window must not aggregate user turns: {sql[:200]}"
        assert "COUNT(CASE" not in sql
    window_sql = [s for s in statements if "COALESCE(s.last_activity_at" in s]
    assert window_sql, "candidate window must order by the indexed effective-activity key"
    assert any("COUNT(m.id) AS actual_message_count" in s for s in fast_sql), (
        "the fast window must keep the exact per-candidate message count"
    )


def test_fast_payload_counts_exclude_claude_code_jsonl_rows_by_design(monkeypatch, tmp_path):
    """The documented count divergence: JSONL-scan rows exist only in the full
    payload's CLI list. The fast payload must not run the scan, so its
    cli_count/cli_session_count cover the bounded state.db window only — and the
    client-read visible rows are unaffected."""
    _install_fixture(monkeypatch, tmp_path)

    jsonl_row = {
        "session_id": "claude_code_deadbeefdeadbeefdeadbeef",
        "title": "JSONL transcript",
        "workspace": "/tmp/fixture-workspace",
        "model": "claude-code",
        "message_count": 4,
        "created_at": T + 3000,
        "updated_at": T + 3010,
        "pinned": False,
        "archived": False,
        "project_id": None,
        "profile": "default",
        "source_tag": "claude_code",
        "raw_source": "claude_code",
        "session_source": "external_agent",
        "source_label": "Claude Code",
        "is_cli_session": True,
        "read_only": True,
    }
    monkeypatch.setattr(
        models, "get_claude_code_sessions", lambda: [dict(jsonl_row)], raising=False,
    )

    full = _build_full()
    fast = _build_fast()

    assert "claude_code_deadbeefdeadbeefdeadbeef" in {r["session_id"] for r in full["sessions"]}
    assert "claude_code_deadbeefdeadbeefdeadbeef" not in {r["session_id"] for r in fast["sessions"]}
    # The JSONL row adds exactly one to the full payload's CLI-for-settings
    # count; the fast payload's count covers the bounded state.db window only.
    assert full["cli_session_count"] == fast["cli_session_count"] + 1
    assert fast["cli_session_count"] > 0  # the fixture's own CLI-classified rows
    # Visible rows still match for the state.db-backed sessions.
    full_ids = {r["session_id"] for r in full["sessions"] if not r["session_id"].startswith("claude_code_")}
    fast_ids = {r["session_id"] for r in fast["sessions"]}
    assert full_ids == fast_ids


def test_fast_window_loader_never_runs_the_jsonl_scan_even_when_asked(monkeypatch, tmp_path):
    """Defense in depth: ``include_claude_code=True`` must not re-enable the
    unbounded JSONL walk on a fast window (the loader forces the skip)."""
    _install_fixture(monkeypatch, tmp_path)
    calls = []

    def _boom():
        calls.append("scan")
        raise AssertionError("the JSONL scan must never run for fast_window=True")

    monkeypatch.setattr(models, "get_claude_code_sessions", _boom, raising=False)
    rows = models.get_cli_sessions(include_claude_code=True, fast_window=True)
    assert calls == []
    assert rows  # the bounded state.db window still returns rows
    assert not any(str(r.get("session_id", "")).startswith("claude_code_") for r in rows)


def test_fast_reader_degrades_without_a_messages_table(monkeypatch, tmp_path):
    """Older/minimal schemas (no usable messages table) must mirror the full
    reader's degradation: denormalized counts, started_at recency."""
    import api.agent_sessions as agent_sessions

    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT, title TEXT, model TEXT, "
        "started_at REAL, message_count INTEGER, last_activity_at REAL, parent_session_id TEXT, "
        "end_reason TEXT, ended_at REAL)"
    )
    conn.execute(
        "INSERT INTO sessions (id, source, title, started_at, message_count, last_activity_at) "
        "VALUES ('legacy-cli', 'cli', 'Legacy session', ?, 3, ?)",
        (T + 10, T + 12),
    )
    conn.commit()
    conn.close()

    rows = agent_sessions.read_fast_sidebar_agent_rows(
        db_path, limit=20, exclude_sources=("cron", "webhook", "kanban"),
    )
    assert [row["id"] for row in rows] == ["legacy-cli"]
    assert rows[0]["actual_message_count"] == 3
    assert rows[0]["last_activity"] is None
    assert rows[0]["message_count"] == 3


def test_fast_payload_is_bounded_and_fills_user_counts_lazily(monkeypatch, tmp_path):
    """The user-turn fallback query must only cover rows dropped by the
    visibility filter, never the whole candidate window."""
    _install_fixture(monkeypatch, tmp_path)
    statements = _record_sqlite_connect(monkeypatch)
    fast = _build_fast()
    assert fast["sessions"]
    user_count_sql = [s for s in statements if "LOWER(role)" in s]
    assert user_count_sql, "the bounded user-count fallback must run for dropped rows"
    for sql in user_count_sql:
        assert "session_id IN" in sql, f"user-count fallback must be id-bounded: {sql[:200]}"
        assert "GROUP BY" in sql


# ── C2: cache wiring ─────────────────────────────────────────────────────────

def _cache_payload(marker, **extra):
    payload = {"sessions": [{"session_id": marker}], "cli_count": 0, "active_profile": None}
    payload.update(extra)
    return payload


def _cache_key():
    return routes._session_list_cache_key(
        active_profile="default",
        all_profiles=False,
        show_cli_sessions=True,
        show_previous_messaging_sessions=False,
        show_cron_sessions=False,
    )


@pytest.fixture(autouse=True)
def _isolated_session_list_cache_state():
    routes._session_list_cache_clear()
    with routes._SESSIONS_CACHE_LOCK:
        routes._SESSIONS_CACHE_INFLIGHT.clear()
    yield
    routes._session_list_cache_clear()
    with routes._SESSIONS_CACHE_LOCK:
        routes._SESSIONS_CACHE_INFLIGHT.clear()


def test_cold_owner_serves_fast_payload_and_rebuilds_full_in_background(monkeypatch):
    monkeypatch.setattr(routes, "_session_list_cache_source_stamp", lambda _key: ("stable",))
    key = _cache_key()
    started = threading.Event()
    release = threading.Event()
    calls = {"full": 0, "fast": 0}

    def _full_builder():
        calls["full"] += 1
        started.set()
        release.wait(5.0)
        return _cache_payload("full")

    def _fast_builder():
        calls["fast"] += 1
        return _cache_payload("fast")

    result = routes._get_cached_session_list_payload(
        key=key, builder=_full_builder, fast_builder=_fast_builder,
    )
    assert result == _cache_payload("fast")
    assert calls == {"full": 1, "fast": 1}
    assert started.wait(1.0), "the full rebuild must run in the background"

    # The fast payload is NOT stored and the rebuild event is still pending, so
    # waiters cannot be released with no fresh payload.
    assert routes._session_list_cache_get(key, allow_stale=True) == (None, False)
    with routes._SESSIONS_CACHE_LOCK:
        event = routes._SESSIONS_CACHE_INFLIGHT.get(key)
    assert event is not None and not event.is_set()

    release.set()
    deadline = time.monotonic() + 5.0
    stored = None
    while time.monotonic() < deadline:
        stored, _fresh = routes._session_list_cache_get(key, allow_stale=True)
        if stored is not None:
            break
        time.sleep(0.02)
    assert stored == _cache_payload("full")
    assert calls == {"full": 1, "fast": 1}


def test_cold_follower_serves_fast_payload_without_waiting_for_owner(monkeypatch):
    monkeypatch.setattr(routes, "_session_list_cache_source_stamp", lambda _key: ("stable",))
    key = _cache_key()
    started = threading.Event()
    release = threading.Event()

    def _full_builder():
        started.set()
        release.wait(5.0)
        return _cache_payload("full")

    owner_result = {}

    def _owner():
        owner_result["payload"] = routes._get_cached_session_list_payload(
            key=key,
            builder=_full_builder,
            fast_builder=lambda: _cache_payload("fast"),
        )

    owner = threading.Thread(target=_owner)
    owner.start()
    try:
        assert started.wait(1.0)
        # The owner's full rebuild is inflight and blocked. A concurrent cold
        # request must return the fast payload instead of waiting 0.25 s and
        # rebuilding synchronously.
        follower = routes._get_cached_session_list_payload(
            key=key,
            builder=_full_builder,
            fast_builder=lambda: _cache_payload("fast"),
        )
        assert follower == _cache_payload("fast")
        assert not release.is_set(), "follower must not block on the owner's rebuild"
    finally:
        release.set()
        owner.join(5.0)
    assert owner_result["payload"] == _cache_payload("fast")


def test_cold_call_without_fast_builder_keeps_synchronous_rebuild(monkeypatch):
    monkeypatch.setattr(routes, "_session_list_cache_source_stamp", lambda _key: ("stable",))
    key = _cache_key()
    calls = []

    def _builder():
        calls.append("full")
        return _cache_payload("full")

    assert routes._get_cached_session_list_payload(key=key, builder=_builder) == _cache_payload("full")
    assert calls == ["full"]


def test_fast_builder_failure_falls_back_to_the_synchronous_full_build(monkeypatch):
    monkeypatch.setattr(routes, "_session_list_cache_source_stamp", lambda _key: ("stable",))
    key = _cache_key()
    calls = []

    def _boom():
        raise RuntimeError("fast build failed")

    def _builder():
        calls.append("full")
        return _cache_payload("full")

    result = routes._get_cached_session_list_payload(
        key=key, builder=_builder, fast_builder=_boom,
    )
    assert result == _cache_payload("full")
    assert calls == ["full"]
    assert routes._session_list_cache_get(key, allow_stale=True)[0] == _cache_payload("full")


@pytest.mark.parametrize("overrides,eligible", [
    ({}, True),
    ({"sidebar_source": "webui"}, True),
    ({"sidebar_source": "cli"}, True),
    ({"all_profiles": True}, False),
    ({"include_archived": True}, False),
    ({"archived_limit": 50}, False),
    ({"source_filter": "cron"}, False),
    ({"visible_only": False}, False),
    ({"sidebar_source": "cron"}, False),
])
def test_fast_shape_gate(overrides, eligible):
    """The gate is about the request SHAPE only: exclude_hidden and the
    show_* settings flags do not change eligibility (parity is asserted for
    those shapes in the payload parity test)."""
    args = dict(
        all_profiles=False,
        visible_only=True,
        include_archived=False,
        archived_limit=None,
        source_filter=None,
        sidebar_source=None,
    )
    args.update(overrides)
    assert routes._session_list_fast_shape_eligible(**args) is eligible


def test_sessions_route_serves_fast_payload_only_for_default_shape(monkeypatch, tmp_path):
    from urllib.parse import urlparse

    class _GetHandler:
        def __init__(self, path):
            self.path = path
            self.headers = {}
            self.client_address = ("127.0.0.1", 12345)
            self.status = None
            from io import BytesIO

            self.wfile = BytesIO()

        def send_response(self, status):
            self.status = status

        def send_header(self, key, value):
            pass

        def end_headers(self):
            pass

        @property
        def response_json(self):
            return json.loads(self.wfile.getvalue().decode("utf-8"))

    _install_fixture(monkeypatch, tmp_path)
    monkeypatch.setattr(routes, "load_settings", lambda: json.loads(
        (tmp_path / "webui" / "settings.json").read_text(encoding="utf-8")
    ))
    monkeypatch.setattr(routes, "_session_list_cache_source_stamp", lambda _key: ("stable",))

    fast_calls = []

    def _fast_builder(**kwargs):
        fast_calls.append(kwargs)
        return _cache_payload("fast", webui_session_count=1, cli_session_count=0)

    monkeypatch.setattr(routes, "_build_session_list_fast_payload", _fast_builder)

    default = _GetHandler("/api/sessions?sidebar_source=webui&exclude_hidden=1")
    routes.handle_get(default, urlparse(default.path))
    assert default.status == 200
    assert [r["session_id"] for r in default.response_json["sessions"]] == ["fast"]
    assert len(fast_calls) == 1

    archive = _GetHandler("/api/sessions?sidebar_source=webui&include_archived=1&archived_limit=50")
    routes.handle_get(archive, urlparse(archive.path))
    assert archive.status == 200
    assert [r["session_id"] for r in archive.response_json["sessions"]] != ["fast"]
    assert len(fast_calls) == 1, "archive/paged shapes must keep the full builder"
