# robinhood-position-tracker

Daily snapshots of a Robinhood brokerage account, stored in SQLite, with two images posted to a
Discord channel: **Positions** (the option book, lots, week change, unrealized and realized P&L) and
**Trades** (contracts opened, added to, trimmed or closed since the previous snapshot, with the
broker's realized gain on each close).

Data is collected through [Robinhood's MCP server](https://agent.robinhood.com/mcp/trading) by the
Claude Code CLI in headless mode, restricted to read-only tools. Nothing here can place, cancel or
modify an order; the write tools are on an explicit deny list on top of the allow list.

## Requirements

- Python 3.10+ and Pillow (`pip install -r requirements.txt`)
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed and logged in
- A Robinhood account connected to the MCP server (done once, interactively; see setup)
- A Discord webhook for the channel that should receive the images
- Fonts: Noto Sans or DejaVu Sans (`fonts-noto-core` / `fonts-dejavu` on Debian and Ubuntu). Anything
  else falls back to Pillow's built-in font. `TRACKER_FONT_DIR` points at a custom directory.

## Setup

1. `.env` in this directory (`chmod 600`):

       TRACKER_ACCOUNTS=123456789:Individual        # number:label, comma-separated for several accounts
       DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

   Account numbers come from `get_accounts` (the `rhs_account_number`); they never go in config.json.
2. Authorize the Robinhood server once: run `claude --mcp-config mcp.json --strict-mcp-config` in this
   directory, type `/mcp`, and complete the login for `robinhood-trading`. Headless runs reuse that login.
3. Test: `python3 tracker.py snapshot` then `python3 tracker.py report` (text) or `report --png`
   (writes `data/reports/`). `report --post` sends the image to Discord.
4. Cron, weekdays after the close (the host's timezone; ET shown):

       30 16 * * 1-5 /path/to/robinhood-position-tracker/run.sh

## Commands

| command | what it does |
|---|---|
| `tracker.py snapshot` | fetch the account through `claude -p`, reconcile against the portfolio totals, store |
| `tracker.py ingest FILE` | store a snapshot JSON that was saved earlier (`data/raw/`) |
| `tracker.py report [--png\|--post]` | the Positions image: print, render, or post |
| `tracker.py changes [--png\|--post] [--force]` | the Trades image for the latest change day |
| `tracker.py daily` | the cron step: on a change day post Positions then Trades (two messages); otherwise post Positions only on `post_weekday` |

`run.sh` wraps snapshot + daily with retries (a second try on the same model, a third on Opus) and
logs to `logs/`.

## What the images show

- **Baseline.** Week change, "new" flags and the realized-since figure compare against the newest
  snapshot at least seven days old (the oldest one while there is less than a week of history).
- **Lots** are rebuilt from the option orders' executions (opening orders are lots, closing fills
  consume them FIFO) and checked against the position's quantity and average.
- **Realized P&L** is the broker's own, lot-matched: per closed trade from the P&L history and a
  year-to-date total. Robinhood's realized data starts in January 2024. A close whose sell trades
  match the detected quantity uses the execution price, the cost of the lots sold and the exact gain;
  otherwise the Trades image marks the estimate with `~`.
- **Reconciliation.** A snapshot whose positions do not sum to the portfolio's own equity and options
  values (within 2%) is rejected rather than stored.

## Config (`config.json`)

| key | meaning |
|---|---|
| `show_dollars` | dollar columns (true) or weights and percentages only (false) |
| `post_weekday` | day for the no-change Positions post, 0 = Monday … 4 = Friday |
| `model` | Claude model for the fetch (`sonnet` is enough; `TRACKER_MODEL` overrides) |
| `mcp_config` | MCP config file with the Robinhood server (`mcp.json`) |
| `bot_name` | webhook display name |

## Files

`data/tracker.db` (SQLite), `data/raw/` (every snapshot as JSON), `data/reports/` (PNGs),
`data/static_cache.json` (contract details, fills and tax lots reused between snapshots), `logs/`.
All of `data/`, `logs/` and `.env` are ignored by git.
