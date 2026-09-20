# Session-list fast first paint (`/api/sessions`)

This document records the current runtime contract for the sidebar session-list
payload: the fast first-paint builder, the full builder's ownership, the
fast/full parity contract, and the fallback behaviors. It describes shipped
behavior and changes no runtime behavior.

## Payload lifecycle

- **Route cache** (`api/route_session_list_cache.py`, wrapped by
  `api/routes._get_cached_session_list_payload`): keyed by the request shape —
  profile scope, `all_profiles`, the `show_*` flags, `include_archived`,
  `exclude_hidden`, `visible_only`, `source_filter`, `sidebar_source`,
  `archived_limit`/`archived_offset`, `show_claude_code_sessions`. TTL 2.5 s
  (45 s while a turn is streaming), max 64 entries.
- **Cache stamp** (`(structural, volatile)`): structural = sessions
  `MAX(rowid)` + session `_index.json` stat + settings stat/version. A
  structural change rebuilds synchronously on the request thread (`"source"`).
  volatile = messages `MAX(rowid)` + state.db/WAL stats + gateway metadata
  stat: the stale payload is served while a background rebuild runs (`"age"`),
  and the entry is never evicted for it.
- **Fast first paint** (`api/routes._build_session_list_fast_payload`): served
  only on a **cold** cache miss (no entry) and only for the default sidebar
  shape (`_session_list_fast_shape_eligible`: `visible_only`, single profile, no
  archive view, no archive paging, no background source filter). It is built
  from bounded indexed reads — webui rows through `all_sessions` with
  `state_db_override_counts=False` (tier-1 primary-key overlay only; no
  `messages` scan on the request thread), CLI/agent rows through
  `read_fast_sidebar_agent_rows` (the bounded candidate window below) plus the
  bounded cron/webhook/kanban chip passes (200 each), and the same Claude Code
  JSONL scan the full builder runs (bounded at `CLAUDE_CODE_MAX_FILES`,
  per-file parse cache) whenever the request shape enables those rows.
  **The fast payload is never stored** in any cache.
- **Full builder** (`_build_session_list_cache_payload`): the only writer of the
  route cache, and (through the non-`fast_window` loader) the only writer of the
  models-layer `get_cli_sessions` cache. On a cold miss the fast payload is
  served while the full payload rebuilds on a daemon thread; the background
  store keeps the stamp the payload was **built** from.
- **Owner/follower**: one request claims the rebuild (owner); concurrent
  requests (followers) serve the fast payload (cold) or the stale payload
  (volatile) without waiting for the owner.

## Fast/full parity contract

- Same payload contract and the same merge→sort→cap tail. Fast visible rows
  equal full visible rows for the same args: ids, order, title, `updated_at`,
  `message_count`, source flags, `project_id`, `pinned`, `archived`,
  `relationship_type`, `parent_session_id`, plus every payload count field
  (`tests/test_session_list_fast_path.py`).
- The fast window keeps the exact per-candidate `COUNT(m.id)` /
  `MAX(m.timestamp)` and the exact `COALESCE(MAX(mx.timestamp), s.started_at)`
  ordering key; only the user-turn aggregation is deferred — rows the
  visibility filter drops get one id-bounded `COUNT` follow-up
  (`_fill_fast_visibility_user_counts`) that reproduces the full projection's
  decision for default-titled CLI rows and ACP rows.
- Documented fast-only divergence: the webui tier-2 `last_message_at` overlay
  (a `messages` aggregation) waits for the background rebuild; the fast paint
  carries the sidecar value until then.

## Candidate window (fast reader)

- The window is a strict prefix of the display order: the candidate CTE orders
  by the exact `COALESCE(MAX(messages.timestamp), started_at)` key — the same
  key the display sorts by — never by the lagging `sessions.last_activity_at`
  (upstream #2662: a session resumed after a long gap ranks at the top by its
  latest message while the denormalized key ranks it past the window).
- SQLite evaluates that ordering key for every qualifying row **before**
  `LIMIT`, so a window ordered directly by it costs one indexed message probe
  per qualifying session at any window size (measured warm medians / cold first
  run: 0.7 ms / 11 ms at 1k sessions, 9.9 ms / 102 ms at 10k, 54.9 ms / 556 ms
  at 50k, 5.3 ms / 271 ms on a clone of the live ~3k-session 4.2 GB store).
  The fast reader therefore seeds the candidate set with a bounded UNION of
  index-ordered pre-windows (`_fast_candidate_union_cte`): top-`candidate_limit`
  (`limit * 8`) by activity, by `started_at`, and by `rowid`, plus the sessions
  of the newest `8 * candidate_limit` message rows (insertion order) — and
  applies the exact key only over that union. The probe count is bounded by the
  pre-window depth at any store size (measured ~0.5-1.2 ms warm), and the final
  exact sort/membership is unchanged.
- The session-row seeds require the agent's standard indexes
  (`idx_sessions_effective_activity` / `idx_sessions_started`). Without them the
  plain exact-key window runs — the pre-union behavior — instead of sorting the
  whole sessions table per pre-window.
- Residual bound: a session whose only recency evidence (its latest message)
  lies outside all four pre-windows is not seeded. Empirically the exact top-N
  over the union equals the exact top-N over all qualifying sessions (checked at
  the 160-row window on the live-store clone and 1k/10k/50k fixtures, and pinned
  by the resumed-session regression tests in
  `tests/test_session_candidate_ordering_perf.py`).
- The full reader keeps the exact-key window over all qualifying sessions: it
  runs in the background rebuild, not on the first-paint path, and it is the
  parity reference the fast window is checked against.

## Fallbacks

- Fast-build failure → the unchanged synchronous full build for that request
  (the fast payload is built before the rebuild event is claimed).
- Archive/paged/`all_profiles`/source-filter shapes, and `visible_only=False`,
  keep the full builder synchronously.
- Legacy schemas: `read_fast_sidebar_agent_rows` mirrors the full reader's
  degradation — no `messages` table → denormalized counts + `started_at`; no
  `messages.session_id` → denormalized counts; no `messages.timestamp` →
  `started_at` window; `limit=None` (the `all_profiles` projection) → delegates
  to the full reader. A read-only open failure returns an empty list; the full
  rebuild recovers the sidebar.
- The fast reader never writes: read-only open, no defensive index self-heal, no
  tombstone/prune bookkeeping — the full rebuild owns those.

## Settings keying

- `show_cli_sessions`, `show_previous_messaging_sessions`, `show_cron_sessions`,
  `show_claude_code_sessions`, `show_webhook_sessions`, and
  `show_kanban_sessions` are part of the route cache key and gate which rows the
  fast payload builds (the JSONL scan included). The settings file stat and the
  settings write version are in the structural stamp, so a settings change
  rebuilds synchronously.
