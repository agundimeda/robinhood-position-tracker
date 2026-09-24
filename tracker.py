#!/usr/bin/env python3
"""Track Robinhood positions over time and post Positions / Trades recaps to Discord.

Data comes from Robinhood's MCP server through `claude -p` (read-only tools only); see README.md.

  tracker.py snapshot              fetch via `claude -p` (read-only MCP tools), store in SQLite
  tracker.py ingest FILE           store an existing snapshot JSON file
  tracker.py report [--post]       print (or post to Discord) the weekly recap
  tracker.py changes [--post|--png]  positions opened, closed or resized since the previous snapshot (PNG like the recap)
  tracker.py daily                 cron step: on a change day post Positions + Trades, else post Positions on post_weekday
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG = json.loads((ROOT / "config.json").read_text())


def _env(key):
    """Read KEY from the environment or ROOT/.env (secrets stay out of config.json and git)."""
    val = os.environ.get(key)
    env = ROOT / ".env"
    if not val and env.exists():
        for line in env.read_text().splitlines():
            if line.startswith(key + "="):
                val = line.split("=", 1)[1].strip().strip('"')
    return val


def _load_accounts():
    """TRACKER_ACCOUNTS=number:label,number:label in .env; config.json holds no account numbers."""
    raw = _env("TRACKER_ACCOUNTS")
    if not raw:
        sys.exit("TRACKER_ACCOUNTS not set (put number:label[,number:label] in .env)")
    accts = []
    for item in raw.split(","):
        number, _, label = item.strip().partition(":")
        accts.append({"number": number, "label": label or "Individual"})
    return accts


CONFIG["accounts"] = _load_accounts()
DB_PATH = ROOT / "data" / "tracker.db"
RAW_DIR = ROOT / "data" / "raw"

READ_TOOLS = [
    "mcp__robinhood-trading__get_portfolio",
    "mcp__robinhood-trading__get_equity_positions",
    "mcp__robinhood-trading__get_option_positions",
    "mcp__robinhood-trading__get_equity_quotes",
    "mcp__robinhood-trading__get_option_quotes",
    "mcp__robinhood-trading__get_option_instruments",
    "mcp__robinhood-trading__get_equity_tax_lots",
    "mcp__robinhood-trading__get_option_orders",
    "mcp__robinhood-trading__get_pnl_trade_history",
    "mcp__robinhood-trading__get_realized_pnl",
]
# Belt and braces: -p mode already denies anything not allowed, but never let a write tool through.
DENY_TOOLS = [
    "Bash", "Write", "Edit", "NotebookEdit", "WebFetch", "WebSearch",
    "mcp__robinhood-trading__place_equity_order",
    "mcp__robinhood-trading__place_option_order",
    "mcp__robinhood-trading__place_crypto_order",
    "mcp__robinhood-trading__place_advanced_order",
    "mcp__robinhood-trading__cancel_equity_order",
    "mcp__robinhood-trading__cancel_option_order",
    "mcp__robinhood-trading__cancel_crypto_order",
    "mcp__robinhood-trading__cancel_advanced_order",
    "mcp__robinhood-trading__exercise_option",
    "mcp__robinhood-trading__cancel_option_exercise",
]
# Snapshot is rejected if position-level values disagree with get_portfolio by more than this.
RECONCILE_TOLERANCE = 0.02

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY,
    snap_date TEXT NOT NULL,
    taken_at TEXT NOT NULL,
    account_number TEXT NOT NULL,
    total_value REAL, equity_value REAL, options_value REAL, crypto_value REAL, cash REAL,
    UNIQUE (snap_date, account_number)
);
CREATE TABLE IF NOT EXISTS equity_positions (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    symbol TEXT, side TEXT, quantity REAL, avg_cost REAL, price REAL, prev_close REAL
);
CREATE TABLE IF NOT EXISTS option_positions (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    option_id TEXT, symbol TEXT, option_type TEXT, side TEXT, strike REAL, expiration TEXT,
    quantity REAL, multiplier REAL, avg_cost REAL, mark REAL, prev_close REAL, opened_at TEXT
);
CREATE TABLE IF NOT EXISTS equity_lots (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    symbol TEXT, open_date TEXT, quantity REAL, cost_per_share REAL
);
CREATE TABLE IF NOT EXISTS option_lots (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    option_id TEXT, open_date TEXT, quantity REAL, cost_per_share REAL
);
CREATE TABLE IF NOT EXISTS realized_trades (
    account_number TEXT NOT NULL, timestamp TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT,
    quantity REAL NOT NULL, price REAL NOT NULL, realized_gain REAL NOT NULL,
    PRIMARY KEY (account_number, timestamp, symbol, quantity, price)
);
CREATE TABLE IF NOT EXISTS change_posts (
    account_number TEXT NOT NULL, snap_date TEXT NOT NULL, digest TEXT NOT NULL, posted_at TEXT NOT NULL,
    PRIMARY KEY (account_number, snap_date)
);
"""


def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(SCHEMA)
    if "opened_at" not in {r["name"] for r in db.execute("PRAGMA table_info(option_positions)")}:
        db.execute("ALTER TABLE option_positions ADD COLUMN opened_at TEXT")
    if "realized_ytd" not in {r["name"] for r in db.execute("PRAGMA table_info(snapshots)")}:
        db.execute("ALTER TABLE snapshots ADD COLUMN realized_ytd REAL")
        db.execute("ALTER TABLE snapshots ADD COLUMN realized_ytd_rate REAL")
        db.execute("ALTER TABLE snapshots ADD COLUMN realized_ytd_trades INTEGER")
    return db


# --- snapshot / ingest -------------------------------------------------------

STATIC_CACHE = ROOT / "data" / "static_cache.json"   # contract details, fills and tax lots (change only on trades)


def load_static_cache():
    return json.loads(STATIC_CACHE.read_text()) if STATIC_CACHE.exists() else {"options": {}, "equities": {}}


def update_static_cache(snap):
    """Remember per-position details that only change when you trade, keyed with the quantity they were valid for."""
    cache = load_static_cache()
    for a in snap["accounts"]:
        for p in a["options"]:
            cache["options"][p["option_id"]] = {k: p[k] for k in ("symbol", "option_type", "strike", "expiration",
                                                                  "multiplier", "quantity") if k in p}
            cache["options"][p["option_id"]].update(opened_at=p.get("opened_at"), fills=p.get("fills", []))
        for p in a["equities"]:
            cache["equities"][p["symbol"]] = {"quantity": p["quantity"], "lots": p.get("lots", [])}
    STATIC_CACHE.parent.mkdir(parents=True, exist_ok=True)
    STATIC_CACHE.write_text(json.dumps(cache, indent=1))


def fetch_snapshot():
    accounts = ", ".join(a["number"] for a in CONFIG["accounts"])
    cached = json.dumps(load_static_cache(), separators=(",", ":"))
    prompt = (ROOT / "fetch_prompt.md").read_text().replace("{accounts}", accounts).replace("{cached}", cached)
    cmd = [
        "claude", "-p", prompt,
        "--output-format", "json",
        "--model", os.environ.get("TRACKER_MODEL") or CONFIG.get("model", "sonnet"),
        "--mcp-config", str(ROOT / CONFIG.get("mcp_config", "mcp.json")),
        "--strict-mcp-config",          # only the Robinhood server from mcp.json, none of the user's other servers
        "--allowedTools", ",".join(READ_TOOLS),
        "--disallowedTools", ",".join(DENY_TOOLS),
    ]
    # Runs in this directory; the CLI reuses the Robinhood login it stored when you first ran `claude` here.
    proc = subprocess.run(cmd, cwd=Path(CONFIG.get("claude_cwd", ROOT)).expanduser(),
                          capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        sys.exit(f"claude exited {proc.returncode}: {proc.stderr[-2000:]}")
    envelope = json.loads(proc.stdout)
    if envelope.get("is_error"):
        sys.exit(f"claude reported an error: {envelope.get('result')}")
    print(f"claude: {envelope.get('num_turns', '?')} turns, ${envelope.get('total_cost_usd', 0):.2f} equivalent")
    text = envelope["result"]
    try:
        # Take the first JSON object and ignore any prose or fences around it.
        snap, _ = json.JSONDecoder().raw_decode(text[text.index("{"):])
        return snap
    except ValueError:
        dump = ROOT / "logs" / f"bad-output-{dt.datetime.now():%Y%m%dT%H%M%S}.txt"
        dump.parent.mkdir(exist_ok=True)
        dump.write_text(text)
        sys.exit(f"claude output was not valid JSON; saved to {dump}")


def local_date(ts):
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone().date().isoformat()


def option_lots(p):
    """Rebuild open lots from fills: each opening order is a lot; closing fills consume lots FIFO."""
    orders = {}
    for f in sorted(p.get("fills", []), key=lambda f: f["timestamp"]):
        o = orders.setdefault(f["order_id"], {"effect": f["effect"], "timestamp": f["timestamp"],
                                              "quantity": 0.0, "notional": 0.0})
        o["quantity"] += f["quantity"]
        o["notional"] += f["quantity"] * f["price"]
    lots = []
    for o in sorted(orders.values(), key=lambda o: o["timestamp"]):
        if o["effect"] == "open":
            lots.append({"open_date": local_date(o["timestamp"]), "quantity": o["quantity"],
                         "cost_per_share": o["notional"] / o["quantity"]})
        else:
            remaining = o["quantity"]
            while remaining > 1e-9 and lots:
                take = min(remaining, lots[0]["quantity"])
                lots[0]["quantity"] -= take
                remaining -= take
                if lots[0]["quantity"] < 1e-9:
                    lots.pop(0)
    return lots


def reconcile(acct):
    """Cross-check the transcribed positions against get_portfolio's own totals."""
    problems = []
    eq = sum(sign(p) * p["quantity"] * p["price"] for p in acct["equities"])
    op = sum(sign(p) * p["quantity"] * p["mark"] * p["multiplier"] for p in acct["options"])
    for name, computed, reported in (("equity", eq, acct["equity_value"]),
                                     ("options", op, acct["options_value"])):
        if abs(computed - reported) > RECONCILE_TOLERANCE * max(abs(reported), 1000):
            problems.append(f"{acct['account_number']} {name}: positions sum to {computed:,.2f}, "
                            f"portfolio reports {reported:,.2f}")
    for p in (p for p in acct["equities"] if "lots" in p):
        lot_qty = sum(l["quantity"] for l in p["lots"])
        if abs(lot_qty - p["quantity"]) > 1e-6:
            problems.append(f"{acct['account_number']} {p['symbol']}: lots sum to {lot_qty:g} shares, "
                            f"position is {p['quantity']:g}")
    for p in (p for p in acct["options"] if "fills" in p):
        lots = option_lots(p)
        lot_qty = sum(l["quantity"] for l in lots)
        lot_avg = sum(l["quantity"] * l["cost_per_share"] for l in lots) / lot_qty if lot_qty else 0
        pos_avg = p["avg_cost"] / p["multiplier"]
        # Robinhood keeps average_price unchanged after a partial sell, so the average is only
        # comparable when no closing fills exist; the quantity check always applies.
        partially_sold = any(f["effect"] == "close" for f in p.get("fills", []))
        if abs(lot_qty - p["quantity"]) > 1e-6 or (not partially_sold and abs(lot_avg - pos_avg) > 0.005 * pos_avg):
            problems.append(f"{acct['account_number']} {p['symbol']} {p['strike']:g}{p['option_type'][0].upper()}: "
                            f"lots give {lot_qty:g} @ {lot_avg:.2f}, position is {p['quantity']:g} @ {pos_avg:.2f}")
    return problems


def ingest(snap):
    problems = [p for a in snap["accounts"] for p in reconcile(a)]
    if problems:
        sys.exit("snapshot failed reconciliation, not stored:\n  " + "\n  ".join(problems))

    taken_at = snap["taken_at"]
    snap_date = local_date(taken_at)
    db = connect()
    with db:
        for a in snap["accounts"]:
            db.execute("DELETE FROM snapshots WHERE snap_date = ? AND account_number = ?",
                       (snap_date, a["account_number"]))
            ytd = a.get("realized_ytd") or {}
            sid = db.execute(
                "INSERT INTO snapshots (snap_date, taken_at, account_number, total_value, equity_value,"
                " options_value, crypto_value, cash, realized_ytd, realized_ytd_rate, realized_ytd_trades)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (snap_date, taken_at, a["account_number"], a["total_value"], a["equity_value"],
                 a["options_value"], a["crypto_value"], a["cash"],
                 ytd.get("realized_gain"), ytd.get("rate"), ytd.get("trades"))).lastrowid
            db.executemany(
                "INSERT OR IGNORE INTO realized_trades VALUES (?,?,?,?,?,?,?)",
                [(a["account_number"], t["timestamp"], t["symbol"], t.get("side"), t["quantity"], t["price"],
                  t["realized_gain"]) for t in a.get("realized_trades", [])])
            db.executemany(
                "INSERT INTO equity_positions VALUES (?,?,?,?,?,?,?)",
                [(sid, p["symbol"], p["side"], p["quantity"], p["avg_cost"], p["price"], p["prev_close"])
                 for p in a["equities"]])
            db.executemany(
                "INSERT INTO option_positions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(sid, p["option_id"], p["symbol"], p["option_type"], p["side"], p["strike"],
                  p["expiration"], p["quantity"], p["multiplier"], p["avg_cost"], p["mark"], p["prev_close"],
                  p.get("opened_at"))
                 for p in a["options"]])
            db.executemany(
                "INSERT INTO equity_lots VALUES (?,?,?,?,?)",
                [(sid, p["symbol"], l["open_date"], l["quantity"], l["cost_per_share"])
                 for p in a["equities"] for l in p.get("lots", [])])
            db.executemany(
                "INSERT INTO option_lots VALUES (?,?,?,?,?)",
                [(sid, p["option_id"], l["open_date"], l["quantity"], l["cost_per_share"])
                 for p in a["options"] if "fills" in p for l in option_lots(p)])
    n = sum(len(a["equities"]) + len(a["options"]) for a in snap["accounts"])
    print(f"stored snapshot {snap_date}: {len(snap['accounts'])} account(s), {n} position(s)")


def sign(p):
    return -1 if p["side"] == "short" else 1


# --- report ------------------------------------------------------------------

def money(x, signed=False):
    s = f"${abs(x):,.0f}"
    if x < 0:
        return "-" + s
    return ("+" + s) if signed else s


def pct(x):
    if x is None:
        return "-"
    v = x * 100
    return f"{v:+,.0f}%" if abs(v) >= 100 else f"{v:+.1f}%"


def ratio(new, old):
    return None if not old else new / old - 1


def tone(x):
    return None if x is None or abs(x) < 1e-9 else ("pos" if x > 0 else "neg")


def load(db, snapshot_id, kind):
    return [dict(r) for r in db.execute(f"SELECT * FROM {kind}_positions WHERE snapshot_id = ?",
                                        (snapshot_id,))]


def option_name(p):
    exp = dt.date.fromisoformat(p["expiration"])
    short = " short" if p["side"] == "short" else ""
    return f"{p['symbol']} {p['strike']:g}{p['option_type'][0].upper()} {exp:%b%y}{short}"


def account_report(db, acct, show_dollars):
    """Compute one account's options recap as a table: header, rows of (kind, cells, tones), closed positions.

    Stock positions are still snapshotted (and reconciled) but left out of the report.
    """
    rows = db.execute("SELECT * FROM snapshots WHERE account_number = ? ORDER BY snap_date",
                      (acct["number"],)).fetchall()
    if not rows:
        return None
    cur = rows[-1]
    cur_date = dt.date.fromisoformat(cur["snap_date"])
    week_ago = (cur_date - dt.timedelta(days=7)).isoformat()
    older = [r for r in rows[:-1] if r["snap_date"] <= week_ago]
    base = older[-1] if older else (rows[0] if len(rows) > 1 else None)

    opts = load(db, cur["id"], "option")
    opt_lots = {}
    for l in db.execute("SELECT * FROM option_lots WHERE snapshot_id = ? ORDER BY open_date", (cur["id"],)):
        opt_lots.setdefault(l["option_id"], []).append(l)
    base_op = {p["option_id"]: p for p in load(db, base["id"], "option")} if base else {}
    total = cur["total_value"]

    weekly = cur_date.weekday() == CONFIG.get("post_weekday", 4)
    subtitle = f"{'Week ending' if weekly else 'As of'} {cur_date:%b %-d, %Y}"
    if base:
        subtitle += f"  ·  vs {dt.date.fromisoformat(base['snap_date']):%b %-d}"
    if len(CONFIG["accounts"]) > 1:
        subtitle += f"  ·  {acct['label']}"

    if show_dollars:
        head = ["", "Qty", "Avg", "Price", "Value", "Bought", "Week", "P&L", "P&L %"]
    else:
        head = ["", "Avg", "Price", "Weight", "Bought", "Week", "P&L %"]

    def position(name, qty, avg, price, value, w, unrl, unrl_pct, bought, kind="position"):
        week = "new" if w is None and base else pct(w)
        if kind == "lot" and week != "new":
            week, w = "", None  # lots move with their contract; only flag lots opened this week
        if show_dollars:
            cells = [name, qty, avg, price, money(value), bought, week, money(unrl, True), pct(unrl_pct)]
            tones = [None, None, None, None, None, None, tone(w), tone(unrl), tone(unrl)]
        else:
            cells = [name, avg, price, f"{value / total * 100:.1f}%", bought, week, pct(unrl_pct)]
            tones = [None, None, None, None, None, tone(w), tone(unrl)]
        return (kind, cells, tones)

    body = []
    tot = {"value": 0.0, "cost": 0.0, "unrl": 0.0, "week_now": 0.0, "week_base": 0.0}
    for p in sorted(opts, key=lambda p: -p["quantity"] * p["mark"] * p["multiplier"]):
        cost = p["quantity"] * p["avg_cost"]
        unrl = sign(p) * (p["quantity"] * p["mark"] * p["multiplier"] - cost)
        b = base_op.get(p["option_id"])
        p_lots = opt_lots.get(p["option_id"], [])
        opened = p_lots[0]["open_date"] if p_lots else (local_date(p["opened_at"]) if p["opened_at"] else "")
        week = ratio(p["mark"], b["mark"]) if b else None
        tot["value"] += sign(p) * p["quantity"] * p["mark"] * p["multiplier"]
        tot["cost"] += cost
        tot["unrl"] += unrl
        if b:  # week change of the total: price move on contracts held at both dates
            tot["week_now"] += sign(b) * b["quantity"] * p["mark"] * p["multiplier"]
            tot["week_base"] += sign(b) * b["quantity"] * b["mark"] * p["multiplier"]
        body.append(position(option_name(p), f"{p['quantity']:g}", f"{p['avg_cost']:,.0f}",
                             f"{p['mark'] * p['multiplier']:,.0f}",
                             sign(p) * p["quantity"] * p["mark"] * p["multiplier"],
                             week, unrl, unrl / cost if cost else None, opened))
        if len(p_lots) > 1:
            for l in p_lots:
                # A lot bought after the baseline is "new"; otherwise the week cell is left blank.
                lot_week = week if base and l["open_date"] <= base["snap_date"] else None
                lot_unrl = sign(p) * l["quantity"] * p["multiplier"] * (p["mark"] - l["cost_per_share"])
                body.append(position("", f"{l['quantity']:g}", f"{l['cost_per_share'] * p['multiplier']:,.0f}", "",
                                     sign(p) * l["quantity"] * p["mark"] * p["multiplier"], lot_week, lot_unrl,
                                     sign(p) * ratio(p["mark"], l["cost_per_share"]), l["open_date"], kind="lot"))

    if opts:
        body.append(position("Total", "", "", "", tot["value"],
                             ratio(tot["week_now"], tot["week_base"]) if tot["week_base"] else None,
                             tot["unrl"], tot["unrl"] / tot["cost"] if tot["cost"] else None, "", kind="total"))

    # Footer: realized P&L from the broker (lot-matched, the app's own numbers). Closes and size changes
    # themselves are covered day-by-day by the Trades image.
    parts = []
    if base:
        wk = db.execute("SELECT COALESCE(SUM(realized_gain), 0) AS g, COUNT(*) AS n FROM realized_trades "
                        "WHERE account_number = ? AND timestamp > ? AND timestamp <= ?",
                        (acct["number"], base["taken_at"], cur["taken_at"])).fetchone()
        if wk["n"]:
            parts.append(f"since {dt.date.fromisoformat(base['snap_date']):%b %-d} {money(wk['g'], True)}")
    if cur["realized_ytd"] is not None:
        ytd = f"YTD {money(cur['realized_ytd'], True)}"
        if cur["realized_ytd_trades"]:
            ytd += f" ({cur['realized_ytd_trades']} closes"
            ytd += f", {pct(cur['realized_ytd_rate'])})" if cur["realized_ytd_rate"] is not None else ")"
        parts.append(ytd)
    footer = ("Realized:  " + "   ·   ".join(parts)) if parts else None
    return {"title": "Positions", "subtitle": subtitle, "head": head, "rows": body, "footer": footer}


# --- position changes --------------------------------------------------------

def position_changes(db, acct):
    """Options opened, closed or resized between the latest snapshot and the previous snapshot day.

    Returns None when there is no earlier snapshot, else a dict with the two dates and a list of
    (kind, name, detail) where kind is "open", "close", "add" or "trim".
    """
    rows = db.execute("SELECT * FROM snapshots WHERE account_number = ? ORDER BY snap_date",
                      (acct["number"],)).fetchall()
    if len(rows) < 2:
        return None
    cur, prev = rows[-1], rows[-2]
    now = {p["option_id"]: p for p in load(db, cur["id"], "option")}
    before = {p["option_id"]: p for p in load(db, prev["id"], "option")}
    lots = {}
    for l in db.execute("SELECT * FROM option_lots WHERE snapshot_id = ? ORDER BY open_date", (cur["id"],)):
        lots.setdefault(l["option_id"], []).append(l)

    def new_lots(oid):
        return [l for l in lots.get(oid, []) if l["open_date"] > prev["snap_date"]]

    def fill(oid, qty):
        ls = new_lots(oid)
        q = sum(l["quantity"] for l in ls)
        return (sum(l["quantity"] * l["cost_per_share"] for l in ls) / q) if q else None

    cache = load_static_cache()["options"]

    def sells(oid):
        """Closing fills since the previous snapshot (from the static cache), as (qty, price)."""
        return [(f["quantity"], f["price"]) for f in cache.get(oid, {}).get("fills", [])
                if f.get("effect") == "close" and f.get("timestamp", "") > prev["taken_at"]]

    def broker_close(symbol, qty):
        """The broker's own realized record for closing `qty` contracts of `symbol` since the previous
        snapshot: (price per contract, realized $) when its sell trades sum to exactly qty, else None."""
        ts = db.execute("SELECT quantity, price, realized_gain FROM realized_trades WHERE account_number = ? "
                        "AND symbol = ? AND side = 'sell' AND timestamp > ? AND timestamp <= ?",
                        (acct["number"], symbol, prev["taken_at"], cur["taken_at"])).fetchall()
        q = sum(t["quantity"] for t in ts)
        if not ts or abs(q - qty) > 1e-9:
            return None
        return sum(t["quantity"] * t["price"] for t in ts) / q, sum(t["realized_gain"] for t in ts)

    changes = []
    for oid, p in now.items():
        b = before.get(oid)
        mult = p["multiplier"]
        if b is None:
            price = fill(oid, p["quantity"]) or p["avg_cost"] / mult
            changes.append(("open", option_name(p), dict(qty=p["quantity"], price=price * mult, avg=p["avg_cost"],
                                                          cost=p["quantity"] * p["avg_cost"], realized=None)))
        elif abs(p["quantity"] - b["quantity"]) > 1e-9:
            d = p["quantity"] - b["quantity"]
            if d > 0:
                price = fill(oid, d) or p["mark"]
                changes.append(("add", option_name(p), dict(delta=d, qty=p["quantity"], price=price * mult,
                                                             avg=p["avg_cost"], cost=d * price * mult, realized=None)))
            else:
                bc = broker_close(p["symbol"], abs(d))
                if bc:   # exact: the broker's lot-matched realized gain and the cost of the lots it sold
                    price_c, realized = bc
                    proceeds = abs(d) * price_c
                    changes.append(("trim", option_name(p), dict(delta=d, qty=p["quantity"], price=price_c,
                                                                  avg=(proceeds - realized) / abs(d), cost=proceeds,
                                                                  realized=realized, approx=False)))
                    continue
                sold = sells(oid)
                q = sum(x for x, _ in sold)
                exact = abs(q - abs(d)) < 1e-9
                price = (sum(x * y for x, y in sold) / q) if exact else p["mark"]
                proceeds = abs(d) * price * mult
                changes.append(("trim", option_name(p), dict(delta=d, qty=p["quantity"], price=price * mult,
                                                              avg=b["avg_cost"], cost=proceeds,
                                                              realized=proceeds - abs(d) * b["avg_cost"], approx=not exact)))
    for oid, b in before.items():
        if oid not in now:
            mult = b["multiplier"]
            bc = broker_close(b["symbol"], b["quantity"])
            if bc:
                price_c, realized = bc
                proceeds = b["quantity"] * price_c
                changes.append(("close", option_name(b), dict(qty=b["quantity"], price=price_c,
                                                              avg=(proceeds - realized) / b["quantity"], cost=proceeds,
                                                              realized=realized, approx=False)))
                continue
            sold = sells(oid)
            q = sum(x for x, _ in sold)
            exact = abs(q - b["quantity"]) < 1e-9
            price = (sum(x * y for x, y in sold) / q) if exact else b["mark"]
            proceeds = b["quantity"] * price * mult
            changes.append(("close", option_name(b), dict(qty=b["quantity"], price=price * mult, avg=b["avg_cost"],
                                                          cost=proceeds, realized=proceeds - b["quantity"] * b["avg_cost"],
                                                          approx=not exact)))
    order = {"open": 0, "add": 1, "trim": 2, "close": 3}
    changes.sort(key=lambda c: (order[c[0]], c[1]))
    return {"date": cur["snap_date"], "prev_date": prev["snap_date"], "changes": changes,
            "total": cur["total_value"], "prev_total": prev["total_value"]}


def changes_text(acct, ch, show_dollars):
    d, pd = dt.date.fromisoformat(ch["date"]), dt.date.fromisoformat(ch["prev_date"])
    lines = [f"{acct['label']} · position changes {d:%b %-d} (since {pd:%b %-d})"]
    width = max(len(name) for _, name, _ in ch["changes"])
    for kind, name, x in ch["changes"]:
        if kind == "open":
            s = f"opened  {x['qty']:g} @ {x['price']:,.0f}"
        elif kind == "add":
            s = f"added   +{x['delta']:g} -> {x['qty']:g} @ {x['price']:,.0f}  avg {x['avg']:,.0f}"
        elif kind == "trim":
            s = f"trimmed {abs(x['delta']):g} -> {x['qty']:g} left @ {x['price']:,.0f}  avg {x['avg']:,.0f}"
        else:
            s = f"closed  {x['qty']:g} @ {x['price']:,.0f}  avg {x['avg']:,.0f}"
        if show_dollars:
            s += f"  {money(x['cost'])}"
            if x["realized"] is not None:
                s += f"  realized {'~' if x.get('approx') else ''}{money(x['realized'], True)}"
        elif x["realized"] is not None and x["avg"]:
            s += f"  realized {'~' if x.get('approx') else ''}{pct(x['price'] / x['avg'] - 1)}"
        lines.append(f"{name.ljust(width)}  {s}")
    return "\n".join(lines)


def changes_report(acct, ch, show_dollars):
    """The change notice in the same table shape as the weekly recap, for render_png."""
    d, pd = dt.date.fromisoformat(ch["date"]), dt.date.fromisoformat(ch["prev_date"])
    head = ["", "Action", "Qty", "Price", "Avg"] + (["Amount", "Realized"] if show_dollars else ["Realized"])
    rows = []
    for kind, name, x in ch["changes"]:
        if kind == "open":
            cells, t = [name, "opened", f"+{x['qty']:g}", f"{x['price']:,.0f}", f"{x['avg']:,.0f}"], "pos"
        elif kind == "add":
            cells, t = [name, "added", f"+{x['delta']:g} -> {x['qty']:g}", f"{x['price']:,.0f}", f"{x['avg']:,.0f}"], "pos"
        elif kind == "trim":
            cells, t = [name, "trimmed", f"{x['delta']:g} -> {x['qty']:g}", f"{x['price']:,.0f}", f"{x['avg']:,.0f}"], "neg"
        else:
            cells, t = [name, "closed", f"-{x['qty']:g}", f"{x['price']:,.0f}", f"{x['avg']:,.0f}"], "neg"
        approx = "~" if x.get("approx") else ""
        if show_dollars:
            cells.append(money(x["cost"]))
            cells.append(approx + money(x["realized"], True) if x["realized"] is not None else "")
        else:
            cells.append(approx + pct(x["price"] / x["avg"] - 1) if x["realized"] is not None and x["avg"] else "")
        tones = [None, t] + [None] * (len(cells) - 3) + [tone(x["realized"])]
        rows.append(("position", cells, tones))
    sub = f"{d:%b %-d, %Y}  ·  since {pd:%b %-d}" + (f"  ·  {acct['label']}" if len(CONFIG["accounts"]) > 1 else "")
    return {"title": "Trades", "subtitle": sub, "head": head, "rows": rows, "footer": None}


def build_changes(show_dollars):
    db = connect()
    out = []
    for a in CONFIG["accounts"]:
        ch = position_changes(db, a)
        if ch and ch["changes"]:
            digest = hashlib.sha1(json.dumps(ch["changes"], sort_keys=True, default=str).encode()).hexdigest()
            out.append((a, ch, digest, changes_text(a, ch, show_dollars)))
    return db, out


def build_reports(show_dollars):
    db = connect()
    return [r for a in CONFIG["accounts"] if (r := account_report(db, a, show_dollars))]


def render_text(r):
    """Monospace preview for the terminal."""
    grid = [r["head"]] + [cells for _, cells, _ in r["rows"]]
    widths = [max(len(row[i]) for row in grid) for i in range(len(grid[0]))]
    lines = ["  ".join(c.ljust(w) if i == 0 else c.rjust(w) for i, (c, w) in enumerate(zip(row, widths))).rstrip()
             for row in grid]
    out = [r["title"], r["subtitle"], ""] + lines
    if r["footer"]:
        out += ["", r["footer"]]
    return "\n".join(out)


# Image styling. Plain dark card; colour is used only for the sign of Week / P&L.
FONT_DIRS = [Path(p) for p in [os.environ.get("TRACKER_FONT_DIR", "")] if p] + [
    Path("/usr/share/fonts/truetype/noto"), Path("/usr/share/fonts/noto"), Path("/usr/share/fonts/truetype/dejavu"),
    Path("/usr/share/fonts/dejavu"), Path("/usr/share/fonts/TTF"), Path("/Library/Fonts"), Path("/System/Library/Fonts"),
]
FONT_FILES = {  # first family found wins; set TRACKER_FONT_DIR to point at a directory holding one of these pairs
    "noto": ("NotoSans-Regular.ttf", "NotoSans-Bold.ttf"),
    "dejavu": ("DejaVuSans.ttf", "DejaVuSans-Bold.ttf"),
}
SCALE = 2  # render at 2x so text stays crisp when Discord scales the image
STYLE = {
    "bg": (24, 25, 28), "text": (230, 231, 234), "muted": (140, 144, 152), "rule": (48, 50, 56),
    "pos": (92, 184, 128), "neg": (226, 98, 96), "pos_dim": (70, 128, 94), "neg_dim": (160, 78, 77),
}


def font(size, bold=False):
    from PIL import ImageFont
    for d in FONT_DIRS:
        for regular, bold_name in FONT_FILES.values():
            p = d / (bold_name if bold else regular)
            if p.exists():
                return ImageFont.truetype(str(p), size * SCALE)
    return ImageFont.load_default()   # legible, if plain; install fonts-noto-core or fonts-dejavu for the real look


def render_png(r, path):
    from PIL import Image, ImageDraw
    s = SCALE
    f_title, f_sub = font(22, bold=True), font(13)
    f_head, f_pos, f_name, f_lot = font(12), font(14), font(14, bold=True), font(12)
    pad, gap, lot_indent = 32 * s, 26 * s, 16 * s
    row_h = {"position": 30 * s, "lot": 22 * s, "total": 34 * s}

    def width(text, f):
        return f.getbbox(text)[2] if text else 0

    def cell_font(kind, i):
        if kind == "lot":
            return f_lot
        return f_name if i == 0 or kind == "total" else f_pos

    ncols = len(r["head"])
    col_w = [width(h, f_head) for h in r["head"]]
    for kind, cells, _ in r["rows"]:
        for i, c in enumerate(cells):
            col_w[i] = max(col_w[i], width(c, cell_font(kind, i)))
    col_w[0] = max(col_w[0], lot_indent)
    table_w = sum(col_w) + gap * (ncols - 1)
    width_px = max(table_w, width(r["title"], f_title), width(r["subtitle"], f_sub)) + 2 * pad

    header_h = 34 * s + 24 * s + 14 * s
    closed_h = 40 * s if r["footer"] else 0
    height_px = pad + header_h + 26 * s + sum(row_h[k] for k, _, _ in r["rows"]) + closed_h + pad

    img = Image.new("RGB", (width_px, height_px), STYLE["bg"])
    d = ImageDraw.Draw(img)
    y = pad
    d.text((pad, y), r["title"], font=f_title, fill=STYLE["text"])
    y += 34 * s
    d.text((pad, y), r["subtitle"], font=f_sub, fill=STYLE["muted"])
    y += 24 * s + 14 * s

    xs = [pad]
    for w in col_w[:-1]:
        xs.append(xs[-1] + w + gap)

    def draw_row(cells, y, h, fonts, fills):
        for i, c in enumerate(cells):
            if not c:
                continue
            f = fonts[i]
            x = xs[i] if i == 0 else xs[i] + col_w[i] - width(c, f)
            d.text((x, y + h / 2), c, font=f, fill=fills[i], anchor="lm")

    draw_row(r["head"], y, 26 * s, [f_head] * ncols, [STYLE["muted"]] * ncols)
    y += 26 * s
    for kind, cells, tones in r["rows"]:
        h = row_h[kind]
        if kind in ("position", "total"):
            d.line([(pad, y), (pad + table_w, y)], fill=STYLE["text" if kind == "total" else "rule"], width=s)
            fills = [STYLE[t] if t else STYLE["text"] for t in tones]
        else:
            fills = [STYLE[t + "_dim"] if t else STYLE["muted"] for t in tones]
        draw_row(cells, y, h, [cell_font(kind, i) for i in range(ncols)], fills)
        y += h
    d.line([(pad, y), (pad + table_w, y)], fill=STYLE["rule"], width=s)

    if r["footer"]:
        d.text((pad, y + 16 * s), r["footer"], font=f_sub, fill=STYLE["muted"])
    img.save(path, optimize=True)


def webhook_url():
    url = _env("DISCORD_WEBHOOK_URL")
    if not url:
        sys.exit("DISCORD_WEBHOOK_URL not set (env var or .env file)")
    return url


def post_text(content):
    """Post a plain-text message (in a code block so columns stay aligned) through the webhook."""
    payload = {"username": CONFIG.get("bot_name", "Position Tracker"), "allowed_mentions": {"parse": []},
               "content": f"```\n{content}\n```"}
    req = urllib.request.Request(webhook_url(), data=json.dumps(payload).encode(), method="POST",
                                 headers={"Content-Type": "application/json",
                                          "User-Agent": "position-tracker (python urllib)"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        if resp.status >= 300:
            sys.exit(f"Discord returned {resp.status}: {resp.read()[:500]}")
    print("posted change notice to Discord")


def post_images(paths, content=None):
    """Upload up to 10 images in one webhook message (multipart/form-data)."""
    boundary = f"----tracker{os.urandom(8).hex()}"
    payload = {"username": CONFIG.get("bot_name", "Position Tracker"), "allowed_mentions": {"parse": []},
               "attachments": [{"id": i, "filename": p.name} for i, p in enumerate(paths)]}
    if content:
        payload["content"] = content
    parts = [(f'Content-Disposition: form-data; name="payload_json"\r\n'
              f"Content-Type: application/json\r\n\r\n").encode() + json.dumps(payload).encode()]
    for i, p in enumerate(paths):
        parts.append((f'Content-Disposition: form-data; name="files[{i}]"; filename="{p.name}"\r\n'
                      f"Content-Type: image/png\r\n\r\n").encode() + p.read_bytes())
    body = b"".join(f"--{boundary}\r\n".encode() + part + b"\r\n" for part in parts) + f"--{boundary}--\r\n".encode()
    req = urllib.request.Request(webhook_url(), data=body, method="POST",
                                 headers={"Content-Type": f"multipart/form-data; boundary={boundary}",
                                          "User-Agent": "position-tracker (python urllib)"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        if resp.status >= 300:
            sys.exit(f"Discord returned {resp.status}: {resp.read()[:500]}")
    print(f"posted {len(paths)} image(s) to Discord")


# --- CLI ---------------------------------------------------------------------

def report_pngs(show):
    out_dir = ROOT / "data" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y-%m-%d")
    paths = []
    for r in build_reports(show):
        p = out_dir / f"{stamp}-{re.sub(r'[^A-Za-z0-9]+', '-', r['title']).strip('-').lower()}.png"
        render_png(r, p)
        paths.append(p)
    return paths


def daily():
    """One cron step after the snapshot. Change day: Positions, then Trades, as two messages. Else Positions on post_weekday."""
    show = CONFIG.get("show_dollars", True)
    db, found = build_changes(show)
    out_dir = ROOT / "data" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    trades, pending = [], []
    for a, ch, digest, text in found:
        done = db.execute("SELECT digest FROM change_posts WHERE account_number = ? AND snap_date = ?",
                          (a["number"], ch["date"])).fetchone()
        if done and done["digest"] == digest:
            print(f"changes for {ch['date']} already posted")
            continue
        print(text)
        png = out_dir / f"{ch['date']}-trades-{re.sub(r'[^A-Za-z0-9]+', '-', a['label']).strip('-').lower()}.png"
        render_png(changes_report(a, ch, show), png)
        trades.append(png)
        pending.append((a, ch, digest))
    today = dt.date.today().weekday()
    if trades:   # two messages: Positions first, then Trades, so each image is shown at full size
        post_images(report_pngs(show))
        post_images(trades)
        with db:
            for a, ch, digest in pending:
                db.execute("INSERT OR REPLACE INTO change_posts VALUES (?,?,?,?)",
                           (a["number"], ch["date"], digest, dt.datetime.now(dt.timezone.utc).isoformat()))
    elif today == CONFIG.get("post_weekday", 4):
        paths = report_pngs(show)
        for i in range(0, len(paths), 10):
            post_images(paths[i:i + 10])
    else:
        print("no changes and not the weekly post day; nothing posted")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("snapshot")
    p_ing = sub.add_parser("ingest")
    p_ing.add_argument("file", type=Path)
    p_rep = sub.add_parser("report")
    out = p_rep.add_mutually_exclusive_group()
    out.add_argument("--post", action="store_true", help="render images and post them to Discord")
    out.add_argument("--png", action="store_true", help="render images to data/reports/ without posting")
    vis = p_rep.add_mutually_exclusive_group()
    vis.add_argument("--show-dollars", dest="show_dollars", action="store_true", default=None)
    vis.add_argument("--hide-dollars", dest="show_dollars", action="store_false")
    p_chg = sub.add_parser("changes")
    p_chg.add_argument("--post", action="store_true", help="post to Discord unless this day's changes were already posted")
    p_chg.add_argument("--png", action="store_true", help="render the notice to data/reports/ without posting")
    p_chg.add_argument("--force", action="store_true", help="post even if already posted")
    sub.add_parser("daily")
    args = ap.parse_args()

    if args.cmd == "snapshot":
        snap = fetch_snapshot()
        update_static_cache(snap)
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        raw = RAW_DIR / f"{dt.datetime.now():%Y-%m-%dT%H%M}.json"
        raw.write_text(json.dumps(snap, indent=2))
        ingest(snap)
    elif args.cmd == "ingest":
        ingest(json.loads(args.file.read_text()))
    elif args.cmd == "daily":
        daily()
    elif args.cmd == "changes":
        show = CONFIG.get("show_dollars", True)
        db, found = build_changes(show)
        if not found:
            print("no position changes since the previous snapshot")
            return
        for a, ch, digest, text in found:
            print(text)
            if not (args.post or args.png):
                continue
            out_dir = ROOT / "data" / "reports"
            out_dir.mkdir(parents=True, exist_ok=True)
            png = out_dir / f"{ch['date']}-changes-{re.sub(r'[^A-Za-z0-9]+', '-', a['label']).strip('-').lower()}.png"
            render_png(changes_report(a, ch, show), png)
            if args.png:
                print(png)
                continue
            done = db.execute("SELECT digest FROM change_posts WHERE account_number = ? AND snap_date = ?",
                              (a["number"], ch["date"])).fetchone()
            if done and done["digest"] == digest and not args.force:
                print(f"already posted for {ch['date']}, skipping")
                continue
            post_images([png])
            with db:
                db.execute("INSERT OR REPLACE INTO change_posts VALUES (?,?,?,?)",
                           (a["number"], ch["date"], digest, dt.datetime.now(dt.timezone.utc).isoformat()))
    elif args.cmd == "report":
        show = CONFIG.get("show_dollars", True) if args.show_dollars is None else args.show_dollars
        reports = build_reports(show)
        if not reports:
            sys.exit("no snapshots yet")
        if args.post or args.png:
            out_dir = ROOT / "data" / "reports"
            out_dir.mkdir(parents=True, exist_ok=True)
            stamp = dt.datetime.now().strftime("%Y-%m-%d")
            paths = []
            for r in reports:
                p = out_dir / f"{stamp}-{re.sub(r'[^A-Za-z0-9]+', '-', r['title']).strip('-').lower()}.png"
                render_png(r, p)
                paths.append(p)
            if args.post:
                for i in range(0, len(paths), 10):
                    post_images(paths[i:i + 10])
            else:
                print("\n".join(str(p) for p in paths))
        else:
            print("\n\n".join(render_text(r) for r in reports))


if __name__ == "__main__":
    main()
