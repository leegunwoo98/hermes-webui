"""Slice D — interactive candidate-window ordering: audit (D0) and swap (D1).

The interactive CLI/agent pass (``read_importable_agent_session_rows`` with
``exclude_sources=("cron", "webhook", "kanban")``) bounds its expensive
messages join to a recency candidate window of ``limit * 8`` rows (the 8x
oversample). D0 audits the metric that matters for swapping the CANDIDATE
ordering from the correlated ``MAX(mx.timestamp)`` subquery to the
denormalized, indexed ``COALESCE(s.last_activity_at, s.started_at)`` key:
candidate-window membership drift. The final display
``ORDER BY COALESCE(MAX(m.timestamp), s.started_at)`` is unchanged by the
swap, so the only risk is a row the pipeline would surface falling outside the
candidate window.

Measured on the live DB by the Slice D review: 0 excluded / 0 extra at the
160-row window for the 20-row slice (worst pipeline-top-20 rank by the
candidate key = 19). Re-verified here in a fixture with realistic
column-vs-join skew (``last_activity_at`` tracks, but lags,
``MAX(messages.timestamp)``: 99.6% of live rows differ at all — seconds for
most rows, but up to ~hours for a handful; live non-NULL median 28.6 s /
p99 490 s / max 10.85 h, NULL-fallback rows up to 17.77 h):
**0 excluded / 0 extra at the 8x oversample, 18/20 top-20 rank churn**.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import api.agent_sessions as agent_sessions

# ---------------------------------------------------------------------------
# Fixture — a state.db whose interactive rows carry realistic skew between the
# denormalized ``sessions.last_activity_at`` column and the exact
# ``MAX(messages.timestamp)`` join, plus the row shapes the pass must tolerate:
# background sources (excluded), zero-message rows (dropped by projection),
# NULL ``last_activity_at`` (fallback to ``started_at``), a subagent pair and a
# compression chain.
# ---------------------------------------------------------------------------

HOT_BASE = 1_800_000_000.0

INTERACTIVE_WHERE = (
    "s.source IS NOT NULL AND s.source NOT IN ('cron','webhook','kanban')"
)
# The exact key the pipeline displays/sorts by (final ORDER BY, unchanged by D1).
EXACT_ORDER = (
    "COALESCE((SELECT MAX(mx.timestamp) FROM messages mx WHERE mx.session_id = s.id),"
    " s.started_at) DESC, s.started_at DESC"
)
# The candidate-window key the D1 swap introduces.
CANDIDATE_ORDER = "COALESCE(s.last_activity_at, s.started_at) DESC, s.started_at DESC"

_SCHEMA = """
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
    last_activity_at REAL
);
CREATE INDEX idx_sessions_effective_activity
    ON sessions(COALESCE(last_activity_at, started_at) DESC, started_at DESC);
CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    role TEXT,
    content TEXT,
    timestamp REAL
);
CREATE INDEX idx_messages_session ON messages(session_id, timestamp);
"""


def _build_state_db(path: Path) -> None:
    """Build the shared Slice D fixture (see module docstring for the shape)."""
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(str(path))
    conn.executescript(_SCHEMA)
    message_seq = 0

    def add(
        sid,
        source,
        started,
        *,
        last_activity_at=None,
        message_count=1,
        parent=None,
        end_reason=None,
        messages=1,
    ):
        nonlocal message_seq
        conn.execute(
            "INSERT INTO sessions (id, source, session_source, title, model, started_at,"
            " message_count, parent_session_id, ended_at, end_reason, last_activity_at)"
            " VALUES (?,?,?,?,?,?,?,?,NULL,?,?)",
            (
                sid,
                source,
                source,
                f"title {sid}",
                "test-model",
                started,
                message_count,
                parent,
                end_reason,
                last_activity_at,
            ),
        )
        for index in range(messages):
            message_seq += 1
            conn.execute(
                "INSERT INTO messages (id, session_id, role, content, timestamp)"
                " VALUES (?,?,?,?,?)",
                (f"msg-{message_seq}", sid, "user", "hi", started + 5 + index),
            )

    # Hot group: 24 rows 7s apart with 0-45s column lag -> heavy rank churn
    # inside the top-20 (18/20 rows change rank between the two keys), while
    # membership stays stable because the whole group is an hour newer than
    # everything else.
    for i in range(24):
        started = HOT_BASE + i * 7
        skew = (i * 13) % 46
        source = {3: "tui", 7: "claude-code", 11: "webui"}.get(i, "desktop")
        add(f"hot-{i:02d}", source, started, last_activity_at=started + 5 - skew)

    # Bulk: 300 rows 5 minutes apart with <=10s lag -> stable membership; the
    # 160-row candidate window boundary lands inside this block.
    bulk_base = HOT_BASE - 3600.0
    for i in range(300):
        started = bulk_base - i * 300
        lag = None if i in (57, 158, 233) else started + 5 - (i % 11)
        add(f"bulk-{i:03d}", "desktop", started, last_activity_at=lag)

    # Zero-message rows: candidate slots that the projection drops.
    for i in range(5):
        started = bulk_base - 200 - i * 300
        add(
            f"empty-{i}",
            "desktop",
            started,
            last_activity_at=started + 1,
            message_count=0,
            messages=0,
        )

    # Subagent parent/child pair (bulk region, outside the top-20).
    add("sub-parent", "subagent", bulk_base - 40 * 300, last_activity_at=bulk_base - 40 * 300 + 5)
    add(
        "sub-child",
        "subagent",
        bulk_base - 41 * 300,
        last_activity_at=bulk_base - 41 * 300 + 5,
        parent="sub-parent",
    )

    # Compression chain root+tip (bulk region): the root's row is what surfaces,
    # carrying the tip's recency.
    add(
        "chain-root",
        "desktop",
        bulk_base - 60 * 300,
        last_activity_at=bulk_base - 60 * 300 + 5,
        end_reason="compression",
    )
    add(
        "chain-tip",
        "desktop",
        bulk_base - 61 * 300,
        last_activity_at=bulk_base - 61 * 300 + 5,
        parent="chain-root",
    )

    # Background sources: newest rows overall, but excluded from this pass.
    for i in range(10):
        add(f"cron-{i}", "cron", HOT_BASE + 400 + i, last_activity_at=HOT_BASE + 400 + i)
        add(f"webhook-{i}", "webhook", HOT_BASE + 500 + i, last_activity_at=HOT_BASE + 500 + i)
        add(f"kanban-{i}", "kanban", HOT_BASE + 600 + i, last_activity_at=HOT_BASE + 600 + i)

    conn.commit()
    conn.close()


def _top_ids(conn: sqlite3.Connection, order_clause: str, limit: int) -> list[str]:
    return [
        str(row[0])
        for row in conn.execute(
            f"SELECT s.id FROM sessions s WHERE {INTERACTIVE_WHERE}"
            f" ORDER BY {order_clause} LIMIT ?",
            (limit,),
        )
    ]


def _interactive_rows(db_path: Path, limit: int = 20):
    return agent_sessions.read_importable_agent_session_rows(
        db_path, limit=limit, exclude_sources=("cron", "webhook", "kanban")
    )


def test_candidate_window_membership_drift_zero_at_eight_x_oversample(tmp_path):
    """D0 audit — fail-closed gate for the D1 swap.

    Metric: the pipeline's exact-key top-20/top-160 rows must all be inside the
    160-row candidate window (8x oversample of the 20-row slice), and no
    candidate row outside the exact top-160 may take a slot. Measured 0/0/0 on
    the live DB; asserted here on the fixture (which shows 18/20 rank churn, so
    the check is not vacuous).
    """
    db = tmp_path / "state.db"
    _build_state_db(db)
    conn = sqlite3.connect(str(db))
    try:
        exact_top20 = _top_ids(conn, EXACT_ORDER, 20)
        exact_top160 = _top_ids(conn, EXACT_ORDER, 160)
        candidate_window = _top_ids(conn, CANDIDATE_ORDER, 160)
    finally:
        conn.close()

    candidate_set = set(candidate_window)
    excluded_top20 = [sid for sid in exact_top20 if sid not in candidate_set]
    excluded_top160 = [sid for sid in exact_top160 if sid not in candidate_set]
    extra_top160 = [sid for sid in candidate_window if sid not in set(exact_top160)]

    # Fixture sanity: the two keys genuinely disagree (skew is real), else the
    # zero-drift assertion below would be vacuous.
    assert candidate_window[:20] != exact_top20

    # The fail-closed gate: any excluded pipeline row means STOP (the swap
    # cannot be validated on this data shape).
    assert excluded_top20 == [], (
        "candidate window (last_activity_at key) excluded pipeline top-20 rows: "
        f"{excluded_top20}"
    )
    assert excluded_top160 == [], (
        "candidate window (last_activity_at key) excluded pipeline top-160 rows: "
        f"{excluded_top160[:10]}"
    )
    assert extra_top160 == [], (
        "candidate window admitted rows outside the pipeline top-160: "
        f"{extra_top160[:10]}"
    )

    # The live pipeline itself (pre-swap code path) must agree with the audit:
    # its visible top-20 equals the exact ordering and stays inside the window.
    visible = _interactive_rows(db, limit=20)
    visible_ids = [str(row["id"]) for row in visible]
    assert visible_ids == exact_top20
    assert set(visible_ids) <= candidate_set


# ---------------------------------------------------------------------------
# D1 — the candidate-ordering swap itself.
#
# The candidate clause becomes ``ORDER BY COALESCE(s.last_activity_at,
# s.started_at) DESC, s.started_at DESC`` (indexed by
# ``idx_sessions_effective_activity``); the final display ORDER BY stays
# ``COALESCE(MAX(m.timestamp), s.started_at)``, so the visible top-N is
# unchanged. Older schemas without ``last_activity_at`` keep the exact
# correlated-subquery candidate ordering.
# ---------------------------------------------------------------------------

# The pre-swap candidate selection, re-implemented as the parity reference.
_OLD_FORM_VISIBLE_SQL = f"""
WITH candidates AS (
    SELECT s.id
    FROM sessions s
    WHERE {INTERACTIVE_WHERE}
    ORDER BY COALESCE(
        (SELECT MAX(mx.timestamp) FROM messages mx WHERE mx.session_id = s.id),
        s.started_at
    ) DESC,
    s.started_at DESC
    LIMIT ?
)
SELECT s.id
FROM sessions s
JOIN candidates c ON c.id = s.id
LEFT JOIN messages m ON m.session_id = s.id
GROUP BY s.id
ORDER BY COALESCE(MAX(m.timestamp), s.started_at) DESC
LIMIT ?
"""


class _RecordingCursor:
    def __init__(self, cursor, executed):
        self._cursor = cursor
        self._executed = executed

    def execute(self, sql, params=()):
        self._executed.append((sql, tuple(params)))
        return self._cursor.execute(sql, params)

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def __iter__(self):
        return iter(self._cursor)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _RecordingConnection:
    def __init__(self, connection, executed):
        self._connection = connection
        self._executed = executed

    def cursor(self):
        return _RecordingCursor(self._connection.cursor(), self._executed)

    def close(self):
        return self._connection.close()

    def commit(self):
        return self._connection.commit()

    @property
    def row_factory(self):
        return self._connection.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._connection.row_factory = value

    def __getattr__(self, name):
        return getattr(self._connection, name)


def _record_connect(monkeypatch, executed):
    real_connect = agent_sessions.sqlite3.connect

    def recording_connect(*args, **kwargs):
        return _RecordingConnection(real_connect(*args, **kwargs), executed)

    monkeypatch.setattr(agent_sessions.sqlite3, "connect", recording_connect)


def test_interactive_candidate_window_orders_by_indexed_effective_activity(monkeypatch, tmp_path):
    """D1: the candidate window is ordered by the denormalized, indexed
    ``COALESCE(s.last_activity_at, s.started_at)`` key with the
    ``s.started_at DESC`` tie-breaker. The correlated per-row
    ``MAX(mx.timestamp)`` subquery is gone from the candidate clause, and the
    final display ORDER BY stays the exact join-based key."""
    db = tmp_path / "state.db"
    _build_state_db(db)

    executed = []
    _record_connect(monkeypatch, executed)
    rows = _interactive_rows(db, limit=20)
    assert rows

    candidate_calls = [(sql, params) for sql, params in executed if "WITH candidates AS" in sql]
    assert candidate_calls, "expected the candidate-window projection SQL"
    candidate_sql, candidate_params = candidate_calls[-1]

    # The swap: indexed expression key + the started_at DESC tie-breaker.
    assert "COALESCE(s.last_activity_at, s.started_at) DESC" in candidate_sql
    assert "s.started_at DESC" in candidate_sql
    # The old correlated per-row subquery is gone from the candidate clause.
    assert "SELECT MAX(mx.timestamp) FROM messages mx WHERE mx.session_id = s.id" not in candidate_sql
    # The final display ordering stays exact (join-based MAX), unchanged.
    assert "ORDER BY COALESCE(MAX(m.timestamp), s.started_at) DESC" in candidate_sql

    # ... and the plan actually uses the expression index, with no correlated
    # scalar subquery left anywhere in the statement.
    conn = sqlite3.connect(str(db))
    try:
        plan = [str(row[-1]) for row in conn.execute("EXPLAIN QUERY PLAN " + candidate_sql, candidate_params)]
    finally:
        conn.close()
    assert any("idx_sessions_effective_activity" in line for line in plan), plan
    assert not any("CORRELATED" in line for line in plan), plan


def test_swapped_candidate_selection_keeps_visible_top_n_parity_under_skew(tmp_path):
    """D1(a): with the column and the join disagreeing by seconds (the live
    shape — 99.6% of rows), the swapped candidate selection yields the same
    visible top-N as the pre-swap correlated-subquery form."""
    db = tmp_path / "state.db"
    _build_state_db(db)
    conn = sqlite3.connect(str(db))
    try:
        exact_top20 = _top_ids(conn, EXACT_ORDER, 20)
        candidate_order_top20 = _top_ids(conn, CANDIDATE_ORDER, 20)
        old_form_visible = [str(row[0]) for row in conn.execute(_OLD_FORM_VISIBLE_SQL, (160, 20))]
    finally:
        conn.close()

    # Fixture sanity: the candidate key genuinely reorders the top-20, so the
    # parity assertion is not vacuous.
    assert candidate_order_top20 != exact_top20

    visible_ids = [str(row["id"]) for row in _interactive_rows(db, limit=20)]

    # Same visible top-N as the old form, in the same (exact-key) order.
    assert visible_ids == old_form_visible
    assert visible_ids == exact_top20


def test_candidate_ordering_falls_back_without_last_activity_column(monkeypatch, tmp_path):
    """Older state.db schemas have no ``sessions.last_activity_at`` column (all
    live DBs do). The pass must keep working there — never referencing the
    missing column — and keep the exact correlated-subquery candidate ordering,
    so a session resumed with a late message still surfaces on top."""
    db = tmp_path / "state.db"
    conn = sqlite3.connect(str(db))
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
            end_reason TEXT
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
        # sid, started_at, message timestamps
        ("old-root", 1000.0, [1000.0, 5000.0]),
        ("old-newer-start", 2000.0, [2000.0]),
        ("old-mid", 1500.0, [1500.0]),
    ]
    for sid, started_at, timestamps in rows:
        conn.execute(
            "INSERT INTO sessions (id, source, session_source, title, model, started_at,"
            " message_count, parent_session_id, ended_at, end_reason)"
            " VALUES (?, 'desktop', 'desktop', ?, 'test-model', ?, 1, NULL, NULL, NULL)",
            (sid, f"title {sid}", started_at),
        )
        for index, timestamp in enumerate(timestamps):
            conn.execute(
                "INSERT INTO messages (id, session_id, role, content, timestamp)"
                " VALUES (?, ?, 'user', 'hi', ?)",
                (f"{sid}-msg-{index}", sid, timestamp),
            )
    conn.commit()
    conn.close()

    executed = []
    _record_connect(monkeypatch, executed)
    result_ids = [str(row["id"]) for row in _interactive_rows(db, limit=20)]

    # Exact recency: the early-started session with the late message ranks first.
    assert result_ids == ["old-root", "old-newer-start", "old-mid"]
    # The missing column is never referenced (it would raise OperationalError,
    # which the caller swallows into an empty sidebar).
    assert all("last_activity_at" not in sql for sql, _params in executed)

