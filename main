# %% [markdown]
# Polymarket — Simple extractor: Tags → Markets (Gamma) → Trade metrics (Data API)
# Outputs ONE set of CSVs (per-tag + combined). No separate analytics file.

# %% [code]
import requests, time, json, csv, unicodedata, sys
from datetime import datetime
from typing import List, Dict, Any, Optional

# ------------------- CONFIG -------------------
GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"

URL_TAGS     = f"{GAMMA_BASE}/tags"
URL_MARKETS  = f"{GAMMA_BASE}/markets"
URL_TRADES   = f"{DATA_BASE}/trades"   # params: market=<conditionId>, takerOnly=false

# Pagination & network
GAMMA_LIMIT = 250
DATA_LIMIT  = 500      # adaptive down to 100 on timeouts
TIMEOUT     = 20
MAX_RETRIES = 5
BACKOFF     = 1.6

# Analysis window (inclusive)
START_ISO = "2025-01-01T00:00:00Z"
START_TS  = int(datetime.fromisoformat(START_ISO.replace("Z","+00:00")).timestamp())

# ------------------- HELPERS -------------------
def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(ch for ch in s if not unicodedata.combining(ch)).lower()

def _get_json(url: str, params: Dict[str, Any]) -> Any:
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            if 200 <= r.status_code < 300:
                return r.json()
            if r.status_code in (408, 429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:240]}")
            r.raise_for_status()
        except Exception as e:
            last = e
            time.sleep(BACKOFF ** (attempt - 1))
    raise RuntimeError(f"GET failed after {MAX_RETRIES} attempts: {last}")

# ------------------- TAGS -------------------
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

def print_tags(tags: List[Dict[str, Any]], max_rows: int = 120):
    if not tags:
        print("No matches found."); return
    print(f"Found {len(tags)} tags (showing up to {max_rows})")
    print("-"*95)
    for i,t in enumerate(tags[:max_rows],1):
        print(f"{i:3d}. id={t.get('id')} | label={t.get('label')} | slug={t.get('slug')}")
    print("-"*95)

# ------------------- GAMMA: MARKETS -------------------
def fetch_markets_by_tag(tag_id: int, closed_flag: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params = {"tag_id": tag_id, "related_tags": "true", "closed": closed_flag,
                  "limit": GAMMA_LIMIT, "offset": offset}
        data = _get_json(URL_MARKETS, params)
        if not isinstance(data, list):
            data = data.get("data", []) if isinstance(data, dict) else []
        out.extend(data)
        if len(data) < GAMMA_LIMIT:
            break
        offset += GAMMA_LIMIT
    return out

def infer_unique_tokens_from_market(m: Dict[str, Any]) -> Optional[int]:
    for key in ("outcomes","answers","tokens","options"):
        v = m.get(key)
        if isinstance(v, list) and v:
            return len(v)
    if isinstance(m.get("num_outcomes"), int):
        return m["num_outcomes"]
    if str(m.get("type","")).lower() == "binary":
        return 2
    return None

# ------------------- DATA API: TRADES (robust cursor) -------------------
def fetch_all_trades_for_condition(condition_id: str) -> List[Dict[str, Any]]:
    trades: List[Dict[str, Any]] = []
    limit = DATA_LIMIT
    next_cursor: Optional[str] = None
    shrinks = 3

    while True:
        params = {"market": condition_id, "takerOnly": "false", "limit": limit}
        if next_cursor:
            params["cursor"] = next_cursor
        try:
            resp = _get_json(URL_TRADES, params)
        except RuntimeError as e:
            msg = str(e).lower()
            if ("408" in msg or "timeout" in msg) and shrinks > 0 and limit > 100:
                shrinks -= 1
                limit = max(100, limit // 2)
                print(f"   [i] Data API timeout → limit {limit}, retry…")
                time.sleep(1.2); continue
            raise

        if isinstance(resp, dict) and "data" in resp:
            items = resp.get("data") or []
            for it in items:
                ts = it.get("timestamp")
                if isinstance(ts, (int,float)) and ts >= START_TS:
                    trades.append(it)
            next_cursor = resp.get("next")
            if not next_cursor or not items:
                break
            continue

        # fallback: raw list + offset pagination if needed
        items = resp if isinstance(resp, list) else []
        for it in items:
            ts = it.get("timestamp")
            if isinstance(ts,(int,float)) and ts >= START_TS:
                trades.append(it)

        if len(items) >= limit:
            offset = limit; shrink_left = 2
            while True:
                params = {"market": condition_id, "takerOnly": "false", "limit": limit, "offset": offset}
                try:
                    resp2 = _get_json(URL_TRADES, params)
                except RuntimeError as e2:
                    m2 = str(e2).lower()
                    if ("408" in m2 or "timeout" in m2) and shrink_left>0 and limit>100:
                        shrink_left -= 1; limit = max(100, limit//2)
                        print(f"   [i] Offset timeout → limit {limit}, retry…"); time.sleep(1.2); continue
                    break
                items2 = resp2 if isinstance(resp2, list) else resp2.get("data", [])
                if not items2: break
                for it in items2:
                    ts = it.get("timestamp")
                    if isinstance(ts,(int,float)) and ts >= START_TS:
                        trades.append(it)
                if len(items2) < limit: break
                offset += limit
        break
    return trades

# ------------------- MARKET METRICS (your requested set) -------------------
def compute_requested_metrics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Required columns:
      traders_count, total_trades, total_volume_usd, avg_trade_size_usd,
      avg_volume_per_trader, avg_trades_per_day, min_trade_size_usd,
      max_trade_size_usd, active_days
    """
    if not trades:
        return {
            "traders_count": 0,
            "total_trades": 0,
            "total_volume_usd": 0.0,
            "avg_trade_size_usd": 0.0,
            "avg_volume_per_trader": 0.0,
            "avg_trades_per_day": 0.0,
            "min_trade_size_usd": 0.0,
            "max_trade_size_usd": 0.0,
            "active_days": 0,
        }

    traders = set()
    vals = []
    first_ts, last_ts = None, None

    for tr in trades:
        price = tr.get("price", 0)
        size  = tr.get("size", 0)
        usd   = abs((size or 0) * (price or 0))
        vals.append(float(usd))
        addr = (tr.get("proxyWallet") or "").lower()
        if addr: traders.add(addr)
        ts = tr.get("timestamp")
        if isinstance(ts,(int,float)):
            ts = int(ts)
            if first_ts is None or ts < first_ts: first_ts = ts
            if last_ts  is None or ts > last_ts:  last_ts  = ts

    total_trades = len(vals)
    total_volume = float(sum(vals))
    traders_cnt  = len(traders)

    min_trade = float(min(vals)) if vals else 0.0
    max_trade = float(max(vals)) if vals else 0.0
    avg_trade = float(total_volume / total_trades) if total_trades else 0.0
    avg_vol_per_trader = float(total_volume / traders_cnt) if traders_cnt else 0.0

    if first_ts and last_ts and last_ts >= first_ts:
        active_days = max(1, int(((last_ts - first_ts)/86400) + 0.999))
    else:
        active_days = 0
    avg_trades_per_day = float(total_trades / active_days) if active_days else 0.0

    return {
        "traders_count": traders_cnt,
        "total_trades": total_trades,
        "total_volume_usd": round(total_volume, 6),
        "avg_trade_size_usd": round(avg_trade, 6),
        "avg_volume_per_trader": round(avg_vol_per_trader, 6),
        "avg_trades_per_day": round(avg_trades_per_day, 6),
        "min_trade_size_usd": round(min_trade, 6),
        "max_trade_size_usd": round(max_trade, 6),
        "active_days": active_days,
    }

# ------------------- SAVE -------------------
def save_markets_dump_for_tag(tag_id: int, markets: List[Dict[str, Any]], stamp: str) -> Dict[str, str]:
    jsonl_path = f"markets_tag_{tag_id}_{stamp}.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for m in markets:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")

    csv_path = f"markets_tag_{tag_id}_{stamp}.csv"
    fields = [
        "id","slug","question","category",
        "liquidity","volume",
        "condition_id","conditionId",
        "created_at","updated_at",
        "unique_tokens",
        # requested metrics:
        "traders_count","total_trades","total_volume_usd",
        "avg_trade_size_usd","avg_volume_per_trader","avg_trades_per_day",
        "min_trade_size_usd","max_trade_size_usd","active_days",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for m in markets:
            w.writerow({
                "id": m.get("id",""),
                "slug": m.get("slug",""),
                "question": m.get("question",""),
                "category": m.get("category",""),
                "liquidity": m.get("liquidity",""),
                "volume": m.get("volume",""),
                "condition_id": m.get("condition_id",""),
                "conditionId": m.get("conditionId",""),
                "created_at": m.get("created_at",""),
                "updated_at": m.get("updated_at",""),
                "unique_tokens": m.get("__unique_tokens",""),
                "traders_count": m.get("__traders_count",""),
                "total_trades": m.get("__total_trades",""),
                "total_volume_usd": m.get("__total_volume_usd",""),
                "avg_trade_size_usd": m.get("__avg_trade_size_usd",""),
                "avg_volume_per_trader": m.get("__avg_volume_per_trader",""),
                "avg_trades_per_day": m.get("__avg_trades_per_day",""),
                "min_trade_size_usd": m.get("__min_trade_size_usd",""),
                "max_trade_size_usd": m.get("__max_trade_size_usd",""),
                "active_days": m.get("__active_days",""),
            })
    return {"jsonl": jsonl_path, "csv": csv_path}

def save_combined_markets_csv(path: str, rows: List[Dict[str, Any]]):
    fields = [
        "tag_id","id","slug","question","category",
        "liquidity","volume",
        "condition_id","conditionId",
        "created_at","updated_at",
        "unique_tokens",
        # requested metrics:
        "traders_count","total_trades","total_volume_usd",
        "avg_trade_size_usd","avg_volume_per_trader","avg_trades_per_day",
        "min_trade_size_usd","max_trade_size_usd","active_days",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for r in rows: w.writerow(r)

# ------------------- MAIN -------------------
try:
    query = input("Enter keyword to search tags (e.g. brazil, argentina, soccer): ").strip()
    if not query: print("Empty query."); raise SystemExit
    tags = find_tags_by_query(query); print_tags(tags, max_rows=200)

    ids_raw = input("Enter one or more tag_id values separated by commas: ").strip()
    if not ids_raw: print("No tag_id provided."); raise SystemExit
    tag_ids = [int(x.strip()) for x in ids_raw.split(",") if x.strip()]

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    print(f"\nAnalysis start from {START_ISO} (epoch {START_TS})")

    combined_rows: List[Dict[str, Any]] = []

    for tid in tag_ids:
        print(f"\n==== Processing tag_id={tid} ====")
        open_mk  = fetch_markets_by_tag(tid, "false"); print(f"  Open markets: {len(open_mk)}")
        closed_mk= fetch_markets_by_tag(tid, "true");  print(f"  Closed markets: {len(closed_mk)}")

        # dedup by market id
        seen=set(); markets=[]
        for m in open_mk+closed_mk:
            mid=str(m.get("id"))
            if mid not in seen:
                seen.add(mid); markets.append(m)
        print(f"  Total markets: {len(markets)}")

        for m in markets:
            # outcomes count (best-effort)
            ut = infer_unique_tokens_from_market(m)
            if ut is not None: m["__unique_tokens"] = int(ut)

            cond = m.get("conditionId") or m.get("condition_id")
            if cond:
                try:
                    trades = fetch_all_trades_for_condition(str(cond))
                except Exception as e:
                    print(f"   [warn] trades fetch failed (conditionId={cond}): {e}")
                    trades = []
                metrics = compute_requested_metrics(trades)
            else:
                metrics = compute_requested_metrics([])

            m["__traders_count"]         = metrics["traders_count"]
            m["__total_trades"]          = metrics["total_trades"]
            m["__total_volume_usd"]      = metrics["total_volume_usd"]
            m["__avg_trade_size_usd"]    = metrics["avg_trade_size_usd"]
            m["__avg_volume_per_trader"] = metrics["avg_volume_per_trader"]
            m["__avg_trades_per_day"]    = metrics["avg_trades_per_day"]
            m["__min_trade_size_usd"]    = metrics["min_trade_size_usd"]
            m["__max_trade_size_usd"]    = metrics["max_trade_size_usd"]
            m["__active_days"]           = metrics["active_days"]

        # save per-tag
        paths = save_markets_dump_for_tag(tid, markets, stamp)
        print(f"Files for tag {tid}: JSONL={paths['jsonl']} | CSV={paths['csv']}")

        # add to combined
        for m in markets:
            combined_rows.append({
                "tag_id": tid,
                "id": m.get("id",""),
                "slug": m.get("slug",""),
                "question": m.get("question",""),
                "category": m.get("category",""),
                "liquidity": m.get("liquidity",""),
                "volume": m.get("volume",""),
                "condition_id": m.get("condition_id",""),
                "conditionId": m.get("conditionId",""),
                "created_at": m.get("created_at",""),
                "updated_at": m.get("updated_at",""),
                "unique_tokens": m.get("__unique_tokens",""),
                # requested metrics:
                "traders_count": m.get("__traders_count",""),
                "total_trades": m.get("__total_trades",""),
                "total_volume_usd": m.get("__total_volume_usd",""),
                "avg_trade_size_usd": m.get("__avg_trade_size_usd",""),
                "avg_volume_per_trader": m.get("__avg_volume_per_trader",""),
                "avg_trades_per_day": m.get("__avg_trades_per_day",""),
                "min_trade_size_usd": m.get("__min_trade_size_usd",""),
                "max_trade_size_usd": m.get("__max_trade_size_usd",""),
                "active_days": m.get("__active_days",""),
            })

    # save combined markets only (no analytics file)
    combined_path = f"markets_all_selected_tags_{stamp}.csv"
    save_combined_markets_csv(combined_path, combined_rows)
    print(f"\nCombined markets CSV saved: {combined_path}")

except KeyboardInterrupt:
    print("\nStopped by user.", file=sys.stderr)
