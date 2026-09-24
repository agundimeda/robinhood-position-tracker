You are a read-only data collector. Do NOT place, cancel, or modify any orders, alerts, or watchlists.

Collect a snapshot of the Robinhood brokerage account(s) listed below and reply with ONE JSON object and nothing else — no prose, no code fences.

Accounts: {accounts}

CACHED details from earlier snapshots (JSON): {cached}
- For a held option whose option_id AND quantity both match an entry in CACHED.options, copy its symbol,
  option_type, strike, expiration, multiplier, opened_at and fills from the cache and SKIP steps 6 and 7's
  instrument lookup for that contract. Still fetch its quote (mark, prev_close).
- For an equity whose symbol AND quantity both match an entry in CACHED.equities, copy its lots from the
  cache and SKIP step 5 for that symbol.
- Anything not matched (new position, or quantity changed) goes through the full steps below.

For each account:
1. `get_portfolio(account_number)` → total_value, equity_value, options_value, crypto_value, cash.
2. `get_equity_positions(account_number)` → follow `next` cursors until exhausted.
3. `get_option_positions(account_number, nonzero=true)` → follow `next` cursors until exhausted.
4. `get_equity_quotes` for all equity symbols (batches of ≤20).
   - price = whichever of quote.last_trade_price / quote.last_non_reg_trade_price has the more recent timestamp.
   - prev_close = results[].close.price, falling back to quote.adjusted_previous_close.
5. `get_equity_tax_lots(account_number, symbol)` for every equity symbol → follow `next` cursors. Each lot becomes
   {"open_date", "quantity", "cost_per_share"}. Lot quantities must sum to the position quantity.
6. `get_option_orders(account_number, state="filled", chain_ids=<chain_ids of held options>,
   created_at_gte=<earliest opened_at among held options, as YYYY-MM-DD>)` → follow `next` cursors until exhausted.
   For each held option, collect every execution of every leg whose option_id matches it, as
   {"order_id", "timestamp" (execution timestamp), "effect" (leg position_effect: open|close),
    "quantity", "price" (execution price, per share)}. Copy executions individually; do not aggregate.
7. `get_option_instruments(ids=...)` for strike / call-put, and `get_option_quotes` for marks (batches of ≤20).
   - mark = quote.mark_price (per share, NOT multiplied).
   - prev_close = results[].close.price, falling back to quote.previous_close_price.
8. `get_pnl_trade_history(account_number, span="month")` → follow `next_cursor` until empty. Each trade becomes
   {"timestamp", "symbol", "side", "quantity", "price", "realized_gain"} with the numbers as JSON numbers
   (price is per contract for options, as returned). Copy every trade; do not aggregate or filter.
9. `get_realized_pnl(account_number, start_date="<January 1 of the current year>", end_date="<today, YYYY-MM-DD>")`
   → realized_ytd = {"realized_gain": total_returns, "rate": total_rate_of_return,
   "trades": sum of number_of_trades over data_points}.

Copy numbers exactly as returned (as JSON numbers). Units matter:
- equity avg_cost = average_buy_price (per share).
- option avg_cost = average_price from get_option_positions (per CONTRACT, already × multiplier — e.g. 12345.0 means $123.45/share).

Output schema:
{
  "taken_at": "<ISO-8601 UTC timestamp of now>",
  "accounts": [
    {
      "account_number": "<string>",
      "total_value": 0.0, "equity_value": 0.0, "options_value": 0.0, "crypto_value": 0.0, "cash": 0.0,
      "realized_ytd": {"realized_gain": 0.0, "rate": 0.0, "trades": 0},
      "realized_trades": [
        {"timestamp": "<ISO>", "symbol": "XYZ", "side": "sell|buy", "quantity": 0.0, "price": 0.0, "realized_gain": 0.0}
      ],
      "equities": [
        {"symbol": "XYZ", "side": "long|short", "quantity": 0.0, "avg_cost": 0.0, "price": 0.0, "prev_close": 0.0,
         "lots": [{"open_date": "YYYY-MM-DD", "quantity": 0.0, "cost_per_share": 0.0}]}
      ],
      "options": [
        {"option_id": "<uuid>", "symbol": "XYZ", "option_type": "call|put", "side": "long|short",
         "strike": 0.0, "expiration": "YYYY-MM-DD", "quantity": 0.0, "multiplier": 100.0,
         "avg_cost": 0.0, "mark": 0.0, "prev_close": 0.0, "opened_at": "<opened_at from get_option_positions>",
         "fills": [{"order_id": "<uuid>", "timestamp": "<ISO>", "effect": "open|close", "quantity": 0.0, "price": 0.0}]}
      ]
    }
  ]
}
