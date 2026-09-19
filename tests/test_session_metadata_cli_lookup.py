from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlparse
import json
import sqlite3
from pathlib import Path


def _make_multi_source_state_db(path: Path) -> None:
    """state.db with three sessions across sources (tui/desktop/telegram)."""
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            source TEXT,
            session_source TEXT,
            title TEXT,
            model TEXT,
            started_at REAL NOT NULL,
            message_count INTEGER DEFAULT 0,
            parent_session_id TEXT,
            ended_at REAL,
            end_reason TEXT,
            user_id TEXT,
            chat_id TEXT,
            chat_type TEXT,
            thread_id TEXT,
            session_key TEXT,
            platform TEXT
        );
        CREATE TABLE messages (
            id TEXT PRIMARY KEY,
            session_id TEXT,
            role TEXT,
            content TEXT,
            timestamp REAL
        );
        CREATE INDEX idx_messages_session ON messages(session_id, timestamp);
        """
    )
    rows = [
        ("tui_session", "tui", "tui", "TUI Session", 30.0),
        ("desktop_session", "desktop", "desktop", "Desktop Session", 20.0),
        ("telegram_session", "telegram", "messaging", "Telegram Session", 10.0),
    ]
    for sid, source, session_source, title, started_at in rows:
        conn.execute(
            "INSERT INTO sessions (id, source, session_source, title, model, started_at,"
            " message_count, parent_session_id, ended_at, end_reason, user_id, chat_id,"
            " chat_type, thread_id, session_key, platform)"
            " VALUES (?, ?, ?, ?, 'test-model', ?, 1, NULL, NULL, NULL, NULL, NULL,"
            " NULL, NULL, NULL, NULL)",
            (sid, source, session_source, title, started_at),
        )
        conn.execute(
            "INSERT INTO messages (id, session_id, role, content, timestamp)"
            " VALUES (?, ?, 'user', 'hello', ?)",
            (f"msg_{sid}", sid, started_at),
        )
    conn.commit()
    conn.close()


def test_read_importable_rows_filters_to_requested_session_ids(tmp_path):
    """A1: session_ids narrows the read to the requested rows only."""
    from api.agent_sessions import read_importable_agent_session_rows

    db = tmp_path / "state.db"
    _make_multi_source_state_db(db)

    bulk = read_importable_agent_session_rows(db, exclude_sources=None)
    bulk_by_id = {row["id"]: row for row in bulk}
    assert set(bulk_by_id) == {"tui_session", "desktop_session", "telegram_session"}

    single = read_importable_agent_session_rows(
        db, exclude_sources=None, session_ids=("tui_session",)
    )

    assert [row["id"] for row in single] == ["tui_session"]
    assert single[0] == bulk_by_id["tui_session"]


def test_read_importable_rows_session_ids_returns_nothing_when_absent(tmp_path):
    from api.agent_sessions import read_importable_agent_session_rows

    db = tmp_path / "state.db"
    _make_multi_source_state_db(db)

    assert read_importable_agent_session_rows(
        db, exclude_sources=None, session_ids=("missing_session",)
    ) == []
    assert read_importable_agent_session_rows(
        db, exclude_sources=None, session_ids=()
    ) == []


def test_read_importable_rows_session_ids_beats_recency_window(tmp_path):
    """The id filter is a WHERE clause, so it wins over the candidate slice.

    This is the property the targeted single-session lookup relies on: the
    single-id read goes through the same candidate-CTE branch as the bulk read
    (no limit bypass) and still returns the requested row even when it is far
    outside the recency window.
    """
    from api.agent_sessions import read_importable_agent_session_rows

    db = tmp_path / "state.db"
    _make_multi_source_state_db(db)

    newest = read_importable_agent_session_rows(db, limit=1, exclude_sources=None)
    assert [row["id"] for row in newest] == ["tui_session"]

    oldest = read_importable_agent_session_rows(
        db, limit=1, exclude_sources=None, session_ids=("telegram_session",)
    )
    assert [row["id"] for row in oldest] == ["telegram_session"]


def test_load_cli_sessions_uncached_threads_session_ids_through_all_passes(monkeypatch, tmp_path):
    """A2: the targeted id set reaches every projection pass (cron/webhook/kanban
    rows are reachable only through their dedicated passes)."""
    import api.models as models

    db = tmp_path / "state.db"
    db.write_text("", encoding="utf-8")
    calls = []

    def fake_read_rows(_db_path, **kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(models, "read_importable_agent_session_rows", fake_read_rows)
    monkeypatch.setattr(
        models,
        "get_claude_code_sessions",
        lambda: (_ for _ in ()).throw(AssertionError("targeted reads must not scan Claude Code JSONL")),
    )

    result = models._load_cli_sessions_uncached(
        tmp_path, db, _cli_profile=None, session_ids=("target_session",)
    )

    assert result == []
    assert len(calls) == 4
    assert all(call.get("session_ids") == ("target_session",) for call in calls)
    assert [call["include_sources"] for call in calls] == [None, ("cron",), ("webhook",), ("kanban",)]


def test_targeted_load_returns_only_requested_rows_and_skips_claude_code_scan(monkeypatch, tmp_path):
    import api.models as models

    db = tmp_path / "state.db"
    _make_multi_source_state_db(db)
    monkeypatch.setattr(models, "get_last_workspace", lambda: tmp_path)
    monkeypatch.setattr(models, "SESSION_DIR", tmp_path / "sessions")
    cc_calls = []

    def fake_claude_code_sessions(*_args, **_kwargs):
        cc_calls.append(1)
        return [{"session_id": "claude_code_should_not_appear", "source_tag": "claude_code"}]

    monkeypatch.setattr(models, "get_claude_code_sessions", fake_claude_code_sessions)

    rows = models._load_cli_sessions_uncached(
        tmp_path, db, _cli_profile=None, session_ids=("telegram_session",)
    )

    assert cc_calls == []
    assert [row["session_id"] for row in rows] == ["telegram_session"]
    assert rows[0]["source_tag"] == "telegram"


def test_load_cli_sessions_uncached_default_still_scans_claude_code(monkeypatch, tmp_path):
    """session_ids=None (the default) preserves the existing bulk behaviour."""
    import api.models as models

    db = tmp_path / "state.db"
    db.write_text("", encoding="utf-8")
    monkeypatch.setattr(models, "read_importable_agent_session_rows", lambda *_a, **_k: [])
    monkeypatch.setattr(
        models,
        "get_claude_code_sessions",
        lambda: [{"session_id": "claude_code_bulk", "source_tag": "claude_code"}],
    )

    rows = models._load_cli_sessions_uncached(tmp_path, db, _cli_profile=None)

    assert [row["session_id"] for row in rows] == ["claude_code_bulk"]


def test_row_builder_characterization_interactive_pass(monkeypatch, tmp_path):
    """A3 characterization: interactive-pass row shape (source-meta merge + title
    fallback + workspace from the memoized resolver)."""
    import api.models as models

    db = tmp_path / "state.db"
    db.write_text("", encoding="utf-8")
    monkeypatch.setattr(models, "get_last_workspace", lambda: tmp_path)
    monkeypatch.setattr(models, "SESSION_DIR", tmp_path / "sessions")
    monkeypatch.setattr(models.Session, "load_metadata_only", lambda _sid: None)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])

    def fake_read_rows(_db_path, **kwargs):
        if kwargs.get("include_sources") is None:
            return [{
                "id": "tui_characterization",
                "title": None,
                "model": "test-model",
                "source": "tui",
                "message_count": 2,
                "actual_message_count": 2,
                "actual_user_message_count": 1,
                "last_activity": 10.0,
                "started_at": 9.0,
            }]
        return []

    monkeypatch.setattr(models, "read_importable_agent_session_rows", fake_read_rows)

    rows = models._load_cli_sessions_uncached(tmp_path, db, _cli_profile="default")

    assert len(rows) == 1
    row = rows[0]
    assert row["session_id"] == "tui_characterization"
    assert row["title"] == "Tui Session"
    assert row["raw_source"] == "tui"
    assert row["session_source"] == "cli"
    assert row["source_label"] == "TUI"
    assert row["is_cli_session"] is True
    assert row["workspace"] == str(tmp_path)
    assert row["created_at"] == 9.0
    assert row["updated_at"] == 10.0
    assert row["project_id"] is None
    assert row["profile"] == "default"


def test_row_builder_characterization_cron_pass_keeps_raw_fields(monkeypatch, tmp_path):
    """A3 characterization: the cron pass keeps RAW row fields (no source-meta
    merge) and its own title fallback."""
    import api.models as models

    db = tmp_path / "state.db"
    db.write_text("", encoding="utf-8")
    monkeypatch.setattr(models, "get_last_workspace", lambda: tmp_path)
    monkeypatch.setattr(models, "SESSION_DIR", tmp_path / "sessions")
    monkeypatch.setattr(models.Session, "load_metadata_only", lambda _sid: None)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(models, "_profile_has_user_projects", lambda: False)
    monkeypatch.setattr(models, "ensure_cron_project", lambda **_: "cron-project-id")

    def fake_read_rows(_db_path, **kwargs):
        if kwargs.get("include_sources") == ("cron",):
            return [{
                "id": "cron_job_characterization_1",
                "title": None,
                "model": "test-model",
                "source": "cron",
                "message_count": 1,
                "actual_message_count": 1,
                "actual_user_message_count": 1,
                "last_activity": 10.0,
                "started_at": 9.0,
            }]
        return []

    monkeypatch.setattr(models, "read_importable_agent_session_rows", fake_read_rows)

    rows = models._load_cli_sessions_uncached(tmp_path, db, _cli_profile="default")

    assert [row["session_id"] for row in rows] == ["cron_job_characterization_1"]
    row = rows[0]
    assert row["title"] == "Cron Session"
    # Raw-field semantics: no normalize_agent_session_source() fallback fill.
    assert row["raw_source"] is None
    assert row["session_source"] is None
    assert row["source_label"] is None
    assert row["source_tag"] == "cron"
    assert row["is_cli_session"] is False
    assert row["project_id"] == "cron-project-id"


def test_row_builder_characterization_webhook_and_kanban_passes(monkeypatch, tmp_path):
    """A3 characterization: webhook/kanban passes merge source meta; the webhook
    pass resolves the workspace per row via get_last_workspace()."""
    import api.models as models

    db = tmp_path / "state.db"
    db.write_text("", encoding="utf-8")
    workspace_marker = tmp_path / "webhook-workspace"
    workspace_calls = []

    def fake_get_last_workspace():
        workspace_calls.append(1)
        return workspace_marker

    monkeypatch.setattr(models, "get_last_workspace", fake_get_last_workspace)
    monkeypatch.setattr(models, "SESSION_DIR", tmp_path / "sessions")
    monkeypatch.setattr(models.Session, "load_metadata_only", lambda _sid: None)
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(models, "ensure_webhook_project", lambda: "webhook-project-id")

    def fake_read_rows(_db_path, **kwargs):
        include = kwargs.get("include_sources")
        if include == ("webhook",):
            return [{
                "id": "webhook_characterization_1",
                "title": None,
                "model": "test-model",
                "source": "webhook",
                "message_count": 1,
                "actual_message_count": 1,
                "actual_user_message_count": 1,
                "last_activity": 10.0,
                "started_at": 9.0,
            }]
        if include == ("kanban",):
            return [{
                "id": "kanban_characterization_1",
                "title": None,
                "model": "test-model",
                "source": "kanban",
                "message_count": 1,
                "actual_message_count": 1,
                "actual_user_message_count": 1,
                "last_activity": 8.0,
                "started_at": 7.0,
            }]
        return []

    monkeypatch.setattr(models, "read_importable_agent_session_rows", fake_read_rows)

    rows = models._load_cli_sessions_uncached(tmp_path, db, _cli_profile="default")
    by_id = {row["session_id"]: row for row in rows}

    webhook = by_id["webhook_characterization_1"]
    assert webhook["title"] == "Webhook Session"
    assert webhook["raw_source"] == "webhook"
    assert webhook["session_source"] == "webhook"
    assert webhook["source_label"] == "Webhook"
    assert webhook["is_cli_session"] is False
    assert webhook["project_id"] == "webhook-project-id"
    # Non-memoized resolver: the webhook pass calls get_last_workspace() per row.
    assert webhook["workspace"] == str(workspace_marker)
    assert workspace_calls

    kanban = by_id["kanban_characterization_1"]
    assert kanban["title"] == "Kanban Session"
    assert kanban["raw_source"] == "kanban"
    assert kanban["session_source"] == "kanban"
    assert kanban["source_label"] == "Kanban"
    assert kanban["is_cli_session"] is False
    assert kanban["project_id"] is None


def test_row_builder_characterization_sidecar_metadata_overrides_title_and_archived(monkeypatch, tmp_path):
    """A3 characterization: sidecar (UI-owned) title/archived win over the
    state.db projection for every pass."""
    import api.models as models

    db = tmp_path / "state.db"
    db.write_text("", encoding="utf-8")
    monkeypatch.setattr(models, "get_last_workspace", lambda: tmp_path)
    monkeypatch.setattr(models, "SESSION_DIR", tmp_path / "sessions")
    monkeypatch.setattr(models, "get_claude_code_sessions", lambda: [])
    monkeypatch.setattr(
        models,
        "_state_projection_sidecar_metadata",
        lambda sid: {"title": "Sidecar Title", "archived": True},
    )

    def fake_read_rows(_db_path, **kwargs):
        if kwargs.get("include_sources") is None:
            return [{
                "id": "tui_sidecar_characterization",
                "title": "State DB Title",
                "model": "test-model",
                "source": "tui",
                "message_count": 1,
                "actual_message_count": 1,
                "actual_user_message_count": 1,
                "last_activity": 10.0,
                "started_at": 9.0,
            }]
        return []

    monkeypatch.setattr(models, "read_importable_agent_session_rows", fake_read_rows)

    rows = models._load_cli_sessions_uncached(tmp_path, db, _cli_profile="default")

    assert len(rows) == 1
    assert rows[0]["title"] == "Sidecar Title"
    assert rows[0]["archived"] is True


def _write_claude_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_lookup_claude_code_session_row_matches_bulk_row_fields(monkeypatch, tmp_path):
    """A5: the single-sid JSONL lookup reproduces every bulk row field."""
    import api.models as models

    projects_dir = tmp_path / "claude" / "projects"
    fixture = projects_dir / "project-a" / "session.jsonl"
    _write_claude_jsonl(fixture, [
        {"summary": "Claude Code lookup QA"},
        {"timestamp": "2026-04-18T12:00:01Z", "message": {"role": "user", "content": "hello"}},
        {"timestamp": "2026-04-18T12:00:02Z", "message": {"role": "assistant", "content": "hi"}},
    ])
    no_ts_fixture = projects_dir / "project-a" / "no-timestamps.jsonl"
    _write_claude_jsonl(no_ts_fixture, [
        {"message": {"role": "user", "content": "no timestamps"}},
    ])
    monkeypatch.setattr(models, "get_last_workspace", lambda: tmp_path / "workspace")

    bulk = {row["session_id"]: row for row in models.get_claude_code_sessions(projects_dir=projects_dir)}
    assert len(bulk) == 2

    for path in (fixture, no_ts_fixture):
        sid = models._claude_code_session_id(path)
        row = models._lookup_claude_code_session_row(sid, projects_dir=projects_dir)
        assert row == bulk[sid], f"field drift for {path.name}"


def test_lookup_claude_code_session_row_skips_message_less_files(monkeypatch, tmp_path):
    """A5: a file whose parse yields no messages stays missing (bulk skips it)."""
    import api.models as models

    projects_dir = tmp_path / "claude" / "projects"
    fixture = projects_dir / "project-a" / "summary-only.jsonl"
    _write_claude_jsonl(fixture, [{"summary": "No messages here"}])
    monkeypatch.setattr(models, "get_last_workspace", lambda: tmp_path)

    sid = models._claude_code_session_id(fixture)
    assert models.get_claude_code_sessions(projects_dir=projects_dir) == []
    assert models._lookup_claude_code_session_row(sid, projects_dir=projects_dir) == {}


def test_lookup_claude_code_session_row_early_exits_on_matching_file(monkeypatch, tmp_path):
    import api.models as models

    projects_dir = tmp_path / "claude" / "projects"
    first = projects_dir / "project-a" / "a.jsonl"
    second = projects_dir / "project-b" / "b.jsonl"
    _write_claude_jsonl(first, [{"message": {"role": "user", "content": "first"}}])
    _write_claude_jsonl(second, [{"message": {"role": "user", "content": "second"}}])
    monkeypatch.setattr(models, "get_last_workspace", lambda: tmp_path)

    row = models._lookup_claude_code_session_row(
        models._claude_code_session_id(second), projects_dir=projects_dir
    )
    assert row["title"] == "second"
    assert row["read_only"] is True
    assert row["profile"] is None
    assert row["source_tag"] == "claude_code"
    assert row["session_source"] == "external_agent"
    assert row["source_label"] == "Claude Code"

    assert models._lookup_claude_code_session_row("claude_code_missing", projects_dir=projects_dir) == {}
    assert models._lookup_claude_code_session_row("not_a_claude_sid", projects_dir=projects_dir) == {}


class _FakeSession:
    def __init__(self, *, is_cli_session=False, session_source=None, source_tag=None):
        self.session_id = "native_webui_001"
        self.title = "Native WebUI"
        self.workspace = "/tmp"
        self.model = "gpt-test"
        self.model_provider = None
        self.messages = []
        self.tool_calls = []
        self.input_tokens = 0
        self.output_tokens = 0
        self.estimated_cost = 0
        self.context_length = 1
        self.threshold_tokens = 0
        self.last_prompt_tokens = 0
        self.active_stream_id = None
        self.pending_user_message = None
        self.pending_attachments = []
        self.pending_started_at = None
        self.composer_draft = {}
        self.is_cli_session = is_cli_session
        self.session_source = session_source
        self.source_tag = source_tag
        self.raw_source = source_tag
        self.source_label = source_tag

    def compact(self):
        return {
            "session_id": self.session_id,
            "title": self.title,
            "workspace": self.workspace,
            "model": self.model,
            "model_provider": self.model_provider,
            "message_count": 0,
            "context_length": self.context_length,
            "threshold_tokens": self.threshold_tokens,
            "last_prompt_tokens": self.last_prompt_tokens,
            "active_stream_id": self.active_stream_id,
            "pending_user_message": self.pending_user_message,
            "composer_draft": self.composer_draft,
            "is_cli_session": self.is_cli_session,
            "session_source": self.session_source,
            "source_tag": self.source_tag,
            "raw_source": self.raw_source,
            "source_label": self.source_label,
        }


def _invoke_api_session(session_obj, *, lookup_cli):
    import api.routes as routes

    captured = {}

    def fake_j(_handler, data, status=200, extra_headers=None):
        captured["data"] = data
        captured["status"] = status
        return data

    parsed = urlparse("/api/session?session_id=native_webui_001&messages=0&resolve_model=0")
    with patch("api.routes.get_session", return_value=session_obj), \
         patch("api.routes._clear_stale_stream_state", return_value=False), \
         patch("api.routes._lookup_cli_session_metadata", side_effect=lookup_cli) as lookup, \
         patch("api.routes.j", side_effect=fake_j):
        routes.handle_get(SimpleNamespace(), parsed)
    return captured, lookup


def test_api_session_metadata_skips_cli_lookup_for_native_webui_session():
    """Native WebUI sessions should not scan Agent state.db on every metadata load."""
    session = _FakeSession()

    def fail_lookup(_sid):
        raise AssertionError("native WebUI metadata should not query CLI sessions")

    captured, lookup = _invoke_api_session(session, lookup_cli=fail_lookup)

    assert captured["status"] == 200
    assert captured["data"]["session"]["session_id"] == "native_webui_001"
    lookup.assert_not_called()


def test_api_session_metadata_keeps_cli_lookup_for_imported_cli_session():
    """Imported CLI/messaging sessions still need Agent metadata for overlap handling."""
    session = _FakeSession(is_cli_session=True, session_source="messaging", source_tag="telegram")

    captured, lookup = _invoke_api_session(
        session,
        lookup_cli=lambda sid: {
            "session_id": sid,
            "session_source": "messaging",
            "source_tag": "telegram",
            "raw_source": "telegram",
            "source_label": "Telegram",
        },
    )

    assert captured["status"] == 200
    assert captured["data"]["session"]["source_tag"] == "telegram"
    lookup.assert_called_once_with("native_webui_001")
