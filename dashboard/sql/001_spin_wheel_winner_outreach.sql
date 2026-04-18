-- 001_spin_wheel_winner_outreach.sql
--
-- Creates analytics_ops schema and the spin-wheel winner outreach tracking table.
-- Applied to Supabase project lzapzucmzvztogacckee on 2026-04-18 via MCP apply_migration
-- (migration name: create_spin_wheel_winner_outreach).
--
-- Idempotent — every CREATE uses IF NOT EXISTS. Re-running is a no-op.
--
-- Nothing in public.* or analytics_prod_* changes. The iOS app (Dracon2) is not aware
-- of analytics_ops and will not read or write it.

create schema if not exists analytics_ops;

create table if not exists analytics_ops.spin_wheel_winner_outreach (
  id                uuid primary key default gen_random_uuid(),
  -- Denormalized natural key pointing back to public.spin_wheel_wins.
  -- No FK: public.spin_wheel_wins is owned by Dracon2 and we don't touch it.
  win_user_id       uuid        not null,
  win_place_id      integer     not null,
  win_created_at    timestamptz not null,
  status            text        not null default 'to_contact'
                    check (status in ('to_contact','contacted','sent','redeemed','skipped')),
  assigned_to       text,
  contacted_at      timestamptz,
  sent_at           timestamptz,
  sent_by           text,
  gift_card_code    text,
  gift_card_value   numeric,
  redeemed_at       timestamptz,
  notes             text,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now(),
  unique (win_user_id, win_place_id, win_created_at)
);

create index if not exists spin_wheel_winner_outreach_status_idx
  on analytics_ops.spin_wheel_winner_outreach (status);

create index if not exists spin_wheel_winner_outreach_nat_key_idx
  on analytics_ops.spin_wheel_winner_outreach (win_user_id, win_place_id, win_created_at);

-- Backfill: create a placeholder outreach row for every existing win.
-- Idempotent: only inserts rows missing from the outreach table.
insert into analytics_ops.spin_wheel_winner_outreach
  (win_user_id, win_place_id, win_created_at, status)
select w.user_id, w.place_id, w.created_at, 'to_contact'
from public.spin_wheel_wins w
left join analytics_ops.spin_wheel_winner_outreach o
  on o.win_user_id    = w.user_id
 and o.win_place_id   = w.place_id
 and o.win_created_at = w.created_at
where o.id is null;
