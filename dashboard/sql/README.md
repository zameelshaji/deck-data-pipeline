# dashboard/sql

Hand-applied Postgres migrations for schemas owned by the dashboard (not by dbt).

These SQL files create state that the Streamlit app reads and writes directly —
they intentionally live outside the dbt project so a `dbt run` never recreates or
truncates them.

## How to apply

Either via the Supabase SQL editor, or via the MCP Supabase tools:

```
mcp__claude_ai_Supabase__apply_migration(
  project_id = "lzapzucmzvztogacckee",
  name       = "<file basename without extension>",
  query      = <file contents>,
)
```

All migrations in this folder are idempotent (`create ... if not exists`,
`insert ... on conflict do nothing`). Re-running is safe.

## Files

- `001_spin_wheel_winner_outreach.sql` — creates `analytics_ops` schema and the
  `spin_wheel_winner_outreach` table used by page 14 (Spin Wheel Winners).
  Applied to `lzapzucmzvztogacckee` on 2026-04-18.
- `002_eqt_insight_memos.sql` — creates `analytics_ops.eqt_insight_memos`,
  an append-only store for EQT-framework insight memos. Populated by scheduled
  Claude triggers (daily/weekly/monthly at 02:00 UTC, after the 01:00 UTC dbt
  rebuild); read by pages 1/2/3 (Daily, Weekly, Monthly) to render the latest
  memo inline for the selected period. Applied to `lzapzucmzvztogacckee` on
  2026-04-18.
