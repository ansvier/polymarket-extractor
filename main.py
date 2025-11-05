# Polymarket Fast Aggregator — Events by Base Slug (robust date anchor)
# Tags → Markets (Gamma) → Aggregated Events (fast-hybrid trades)
# Merges outcomes into one event, fetches trades for ONE representative market per event.

import requests, time, json, csv, unicodedata, re, sys
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# =================== CONFIG ===================
GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"
POLY_BASE  = "https://polymarket.com"

URL_TAGS     = f"{GAMMA_BASE}/tags"
URL_MARKETS  = f"{GAMMA_BASE}/markets"
URL_TRADES   = f"{DATA_BASE}/trades"

GAMMA_LIMIT = 250
DATA_LIMIT  = 250            # conservative to minimize 408/504
TIMEOUT     = 20
MAX_RETRIES = 5
BACKOFF     = 1.8

# Analysis window (set to None for full history)
START_ISO = "2025-01-01T00:00:00Z"
START_TS  = int(datetime.fromisoformat(START_ISO.replace("Z","+00:00")).timestamp()) if START_ISO else None

# =================== HELPERS ===================
def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(ch for ch in s if not unicodedata.combining(ch)).lower()

def _get_json(url: str, params: Dict[str, Any]) -> Any:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if 200 <= r.status_code < 300:
                return r.json()
            if r.status_code in (408, 429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:240]}")
            r.raise_for_status()
        except Exception as e:
            last_error = e
            time.sleep(BACKOFF ** (attempt - 1))
    raise RuntimeError(f"GET failed after {MAX_RETRIES} attempts: {last_error}")

# =================== TAGS ===================
def find_tags_by_query(query: str) -> List[Dict[str, Any]]:
    q = _norm(query)
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        data = _get_json(URL_TAGS, {"limit": GAMMA_LIMIT, "offset": offset})
        if not isinstance(data, list):
            data = data.get("data", []) if isinstance(data, dict) else []
        for t in data:
            if q in _norm(t.get("label","")) or q in _norm(t.get("slug","")):
                out.append(t)
        if len(data) < GAMMA_LIMIT:
            break
        offset += GAMMA_LIMIT
    return out

def print_tags(tags: List[Dict[str, Any]], max_rows: int = 150):
    if not tags:
        print("No matches found."); return
    print(f"Found {len(tags)} tags (showing up to {max_rows})")
    print("-"*95)
    for i,t in enumerate(tags[:max_rows],1):
        print(f"{i:3d}. id={t.get('id')} | label={t.get('label')} | slug={t.get('slug')}")
    print("-"*95)

# =================== GAMMA ===================
def fetch_markets_by_tag(tag_id: int, closed_flag: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params = {
            "tag_id": tag_id, "related_tags": "true",
            "closed": closed_flag, "limit": GAMMA_LIMIT, "offset": offset
        }
        data = _get_json(URL_MARKETS, params)
        if not isinstance(data, list):
            data = data.get("data", []) if isinstance(data, dict) else []
        out.extend(data)
        if len(data) < GAMMA_LIMIT:
            break
        offset += GAMMA_LIMIT
    return out

# =================== BASE SLUG (robust date anchor) ===================
ONE_TOKEN_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
YEAR_RE  = re.compile(r"^\d{4}$")
MONTH_RE = re.compile(r"^(0[1-9]|1[0-2])$")
DAY_RE   = re.compile(r"^(0[1-9]|[12]\d|3[01])$")

def _find_last_date_span(parts: List[str]) -> Optional[Tuple[int,int]]:
    """
    Return (start_idx, end_idx) for the LAST date span in parts.
    Supports:
      - single token 'YYYY-MM-DD'
      - three tokens 'YYYY','MM','DD'
    """
    last_span: Optional[Tuple[int,int]] = None
    n = len(parts)
    for i, p in enumerate(parts):
        if ONE_TOKEN_DATE_RE.match(p):
            last_span = (i, i)
        if i + 2 < n and YEAR_RE.match(parts[i]) and MONTH_RE.match(parts[i+1]) and DAY_RE.match(parts[i+2]):
            last_span = (i, i+2)
    return last_span

def base_slug(slug: str) -> str:
    """Trim everything AFTER the last detected date span; if no date, return slug unchanged."""
    if not slug:
        return ""
    parts = slug.split("-")
    span = _find_last_date_span(parts)
    if span:
        _, end = span
        return "-".join(parts[:end+1])
    return slug

def event_url(bslug: str) -> str:
    return f"{POLY_BASE}/event/{bslug}" if bslug else ""

# =================== TRADES (representative market only) ===================
def fetch_trades_for_market(condition_id: str) -> List[Dict[str, Any]]:
    trades: List[Dict[str, Any]] = []
    next_cursor = None
    limit = DATA_LIMIT
    retries_left = 3
    while True:
        params = {"market": condition_id, "takerOnly": "false", "limit": limit}
        if next_cursor:
            params["cursor"] = next_cursor
        try:
            resp = _get_json(URL_TRADES, params)
        except RuntimeError as e:
            msg = str(e).lower()
            if ("timeout" in msg or "408" in msg) and retries_left > 0 and limit > 100:
                retries_left -= 1
                limit = max(100, limit // 2)
                print(f"   [i] retry trades, new limit={limit}")
                continue
            break
        data = resp.get("data") if isinstance(resp, dict) else resp
        if not data:
            break
        for t in data:
            ts = t.get("timestamp")
            if START_TS is None or (isinstance(ts,(int,float)) and ts >= START_TS):
                trades.append(t)
        next_cursor = resp.get("next") if isinstance(resp, dict) else None
        if not next_cursor:
            break
    return trades

def compute_metrics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not trades:
        return dict(traders_count=0,total_trades=0,total_volume_usd=0,
                    avg_trade_size_usd=0,avg_volume_per_trader=0,
                    avg_trades_per_day=0,min_trade_size_usd=0,
                    max_trade_size_usd=0,active_days=0,_traders=set())
    vals = []
    traders = set()
    first_ts, last_ts = None, None
    for tr in trades:
        size = float(tr.get("size",0) or 0)
        price = float(tr.get("price",0) or 0)
        usd = abs(size * price)
        vals.append(usd)
        addr = (tr.get("proxyWallet") or "").lower()
        if addr: traders.add(addr)
        ts = tr.get("timestamp")
        if isinstance(ts,(int,float)):
            if first_ts is None or ts < first_ts: first_ts = ts
            if last_ts is None or ts > last_ts: last_ts = ts
    total = sum(vals)
    n = len(vals)
    tcnt = len(traders)
    minv = min(vals) if vals else 0
    maxv = max(vals) if vals else 0
    avgsize = total / n if n else 0
    avgvoltr = total / tcnt if tcnt else 0
    active = int((last_ts-first_ts)/86400)+1 if first_ts and last_ts else 0
    avgday = n/active if active else 0
    return dict(traders_count=tcnt,total_trades=n,total_volume_usd=round(total,6),
                avg_trade_size_usd=round(avgsize,6),avg_volume_per_trader=round(avgvoltr,6),
                avg_trades_per_day=round(avgday,6),min_trade_size_usd=round(minv,6),
                max_trade_size_usd=round(maxv,6),active_days=active,_traders=traders)

# =================== SAVE ===================
def save_events_csv(path: str, rows: list):
    # Column order fixed to match your sheet
    fields = [
        "event_key","url","markets_count","unique_tokens",
        "total_volume_usd","traders_count","total_trades",
        "avg_trade_size_usd","avg_volume_per_trader","avg_trades_per_day",
        "min_trade_size_usd","max_trade_size_usd","active_days",
        "rep_market_id","rep_condition_id","unique_events","any_question"
    ]
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader()
        for r in rows: w.writerow(r)

# =================== AGGREGATION ===================
def aggregate_events(markets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Group markets into events by robust base slug (trim after last date)
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for m in markets:
        slug = str(m.get("slug",""))
        bslug = base_slug(slug)
        groups.setdefault(bslug, []).append(m)

    rows = []
    unique_events = len(groups)

    for bslug, mkts in groups.items():
        # Representative = market with maximum Gamma volume
        rep = max(mkts, key=lambda x: float(x.get("volume",0) or 0.0))
        cond = rep.get("conditionId") or rep.get("condition_id")
        trades = fetch_trades_for_market(cond) if cond else []
        mm = compute_metrics(trades)

        rows.append({
            "event_key": bslug,
            "url": event_url(bslug),
            "markets_count": len(mkts),
            "unique_tokens": len(mkts),
            "total_volume_usd": round(sum(float(m.get("volume",0) or 0.0) for m in mkts), 6),  # exact Gamma sum
            "traders_count": mm["traders_count"],
            "total_trades": mm["total_trades"],
            "avg_trade_size_usd": mm["avg_trade_size_usd"],
            "avg_volume_per_trader": mm["avg_volume_per_trader"],
            "avg_trades_per_day": mm["avg_trades_per_day"],
            "min_trade_size_usd": mm["min_trade_size_usd"],
            "max_trade_size_usd": mm["max_trade_size_usd"],
            "active_days": mm["active_days"],
            "rep_market_id": rep.get("id",""),
            "rep_condition_id": cond or "",
            "unique_events": unique_events,
            "any_question": rep.get("question",""),
        })
    return rows

# =================== MAIN ===================
try:
    query = input("Enter keyword (e.g. argentina, mexico, soccer): ").strip()
    if not query: raise SystemExit("Empty query.")
    tags = find_tags_by_query(query)
    print_tags(tags)
    ids_raw = input("Enter tag_id(s), comma-separated: ").strip()
    if not ids_raw: raise SystemExit("No tag_id provided.")
    tag_ids = [int(x.strip()) for x in ids_raw.split(",") if x.strip()]
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    all_markets: List[Dict[str, Any]] = []
    for tid in tag_ids:
        print(f"\n==== tag {tid} ====")
        open_mk  = fetch_markets_by_tag(tid, "false")
        closed_mk= fetch_markets_by_tag(tid, "true")
        # de-duplicate by market id
        mkts = {m["id"]: m for m in open_mk + closed_mk if "id" in m}.values()
        all_markets.extend(mkts)
        print(f"  markets total: {len(mkts)}")

    print("\nAggregating events ...")
    events = aggregate_events(list(all_markets))
    out_csv = f"events_aggregated_{stamp}.csv"
    save_events_csv(out_csv, events)
    print(f"Saved aggregated events: {out_csv}")

except KeyboardInterrupt:
    print("\nStopped by user.")
