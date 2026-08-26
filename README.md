# ATR Poker Backend — Phase 1

This is the first server-backed multiplayer layer for ATR Poker.

## What Phase 1 does

- validates the existing Supabase Auth session on every Socket.IO connection;
- creates/reads a server-side play-money wallet;
- exposes live lobby occupancy;
- seats real ATR accounts at a shared table;
- synchronizes seat state to everyone in that room;
- supports realtime table chat;
- cashes the table stack back to the server wallet on Stand Up.

The existing browser poker engine is intentionally **not authoritative yet**. Cards, RNG, betting and pot settlement remain local until Phase 2, when they move to the server.

## Setup

1. Run `sql/001_poker_multiplayer.sql` in the Supabase SQL editor.
2. Copy `.env.example` to `.env`.
3. Put the existing Supabase URL/publishable key in `.env`.
4. Put the **service role key only in `.env` on the backend**. Never paste it into HTML.
5. Run:
   - `npm install`
   - `npm run dev`
6. Serve the ATR HTML through a local web server (for example port 5500).
7. Open `ATR_V13_0_MULTIPLAYER_FOUNDATION.html`.

## Important

Phase 1 is a foundation, not the final secure poker engine. The join/cash-out path should move to a Postgres RPC/transaction before production. Phase 2 will also move deck generation, turns, timers, bets, pots, side pots, showdown and hand history to the server.


## V13.3 — Authoritative Deal Test
- Server securely shuffles with Node crypto.randomInt.
- Dealer/SB/BB and preflop actor are server-authoritative.
- Hole cards are sent privately to each player's socket only.
- Fold/Check/Call/Raise remain disabled until the next server-action phase.
- `.env` is intentionally NOT included in this ZIP; copy your existing `.env` into the new folder.


## V13.4
Server-authoritative fold/check/call/raise, flop/turn/river, showdown, payout, automatic next hand. Full side pots are not yet implemented. Copy your existing .env before starting.


## V13.4.1 Fix
- Hole-card deal animation only runs once per hand.
- Betting round waits for every actionable player before advancing street.


## V13.4.2 Winner / Showdown Fix
- Winner banner stays visible for 3.5 seconds before next hand.
- Fold wins show the winner and pot amount; folded hole cards stay hidden.
- Showdown reveals only showdown contenders after the hand ends.
- Table state refresh is delayed so the completed hand is readable.


## V13.4.3 Result Sync Fix
- Finished hands remain registered during the 3.5s result hold.
- Prevents stale River UI / NO_ACTIVE_HAND desync.
- Stale active packets from a completed hand are ignored client-side.
- Completed hands disable actions immediately, then next hand starts after hold.


## V13.4.4 Final State Fix
- Fixes hard-coded handActive:true in handPublicState for completed hands.
- Action ACK carries authoritative state, including winner/showdown state.
- HAND_COMPLETE triggers an immediate final-state resync instead of leaving River stuck.
- poker:hand:get returns completed winner state during the result hold.


## V13.5 Server Turn Timer
- 20-second authoritative timer on the server for every player action.
- Timeout with nothing to call => AUTO-CHECK.
- Timeout facing a bet => AUTO-FOLD.
- All clients render the same server deadline around the active avatar.


## V13.5.1 Turn Timer Fix
- Fixes server timeout crash caused by a wrong helper name (`playerBySeat` -> `bySeat`).
- AUTO-CHECK / AUTO-FOLD now executes when the 20-second server timer expires.


## V13.6 All-In + Side Pots
- Tracks total contribution per player across all streets.
- True all-in raises/calls supported.
- Short all-in raises are accepted without reopening betting.
- Showdown creates and settles main/side pots independently for up to 6 players.
- Side-pot winners can differ by pot.


## V13.6.1 All-In Button Fix
- ALL-IN now executes immediately in multiplayer instead of only filling the raise input.
- Short-stack ALL-IN becomes a call when it cannot exceed the current bet.


## V13.6.2 Leave After Hand Fix
- LEAVE SEAT during an active multiplayer hand now arms leave-after-hand.
- After the result, the player's remaining table stack is returned to the wallet.
- The seat is removed before the next hand is scheduled.
- The client automatically returns to the lobby.
