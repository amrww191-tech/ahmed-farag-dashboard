"""
╔══════════════════════════════════════════════════════════════╗
║    Ahmed Farag Pro - Telegram Bot (ATAS Aggregated Edition)  ║
║    نظام مسح ذكي فائق السرعة + رادار التصفيات المجمعة         ║
╚══════════════════════════════════════════════════════════════╝
"""

import os, sys, io, asyncio, logging, requests, numpy as np, time
import concurrent.futures, threading, collections, json
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import websockets

# ─── إصلاح الترميز على Windows ─────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ─── مكتبة الصور والرسم البياني ────────────────────────────
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, WebAppInfo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

# ══════════════════════════════════════════════════════════════
# ⚙️  إعدادات البوت والاتصال
# ══════════════════════════════════════════════════════════════
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    try:
        with open(_env_path, "r", encoding="utf-8") as _ef:
            for _line in _ef:
                _line = _line.strip()
                if _line and not _line.startswith("#") and "=" in _line:
                    _k, _v = _line.split("=", 1)
                    os.environ.setdefault(_k.strip(), _v.strip())
    except Exception:
        pass

BOT_TOKEN       = os.getenv("BOT_TOKEN", "").strip()
ALLOWED_CHAT_ID = int(os.getenv("ALLOWED_CHAT_ID", "1686148743").strip() or 0)
DASHBOARD_URL   = 'https://amrww191-tech.github.io/ahmed-farag-dashboard/'
PORT            = int(os.getenv("PORT", "8080"))

EXCLUDED_COINS = [
    'BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT','USDCUSDT',
    'ADAUSDT','DOGEUSDT','TRXUSDT','AVAXUSDT','LINKUSDT','MATICUSDT',
    'DOTUSDT','LTCUSDT','BCHUSDT','UNIUSDT','PEPEUSDT','SHIBUSDT'
]

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("MDBorsaBot")

# كاش ذكي
_LAST_SCAN_TIME   = 0
_CACHED_RESULTS   = None
_CACHED_FUNDING   = None
_AUTO_ALERTS_ON   = True
_ALERT_INTERVAL   = 45   # دقيقة
_SCAN_LOCK        = threading.Lock()

# ─── حوض اتصالات فائق السرعة Connection Pool ─────────────────
_HTTP_SESSION = requests.Session()
_adapter = HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=Retry(total=2, backoff_factor=0.1))
_HTTP_SESSION.mount("https://", _adapter)
_HTTP_SESSION.mount("http://", _adapter)

# ══════════════════════════════════════════════════════════════
# ⚡ LiveLiquidationEngine (ATAS / Coinglass WebSocket Aggregator)
# ══════════════════════════════════════════════════════════════
class LiveLiquidationEngine:
    """
    محرك تجميع التصفيات المباشرة لحظياً بنموذج ATAS و Coinglass.
    يتصل مباشرة بـ WebSocket لمنصة بينانس (fstream.binance.com)
    ويرصد كافة أحجام التصفية القسرية market-wide بدقة متناهية.
    """
    def __init__(self, window_1h_sec=3600):
        self.window_1h = window_1h_sec
        self._lock = threading.Lock()
        # Events: (timestamp_sec, symbol, side, qty, price, usd_val)
        self._events = collections.deque(maxlen=200000)
        self._running = False
        self._thread = None
        self._connected = False

    def start(self):
        if self._running: return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="LiquidationEngine")
        self._thread.start()
        logger.info("LiveLiquidationEngine started in background thread.")

    def _run_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._ws_listener())

    async def _ws_listener(self):
        url = "wss://fstream.binance.com/ws/!forceOrder@arr"
        while self._running:
            try:
                logger.info(f"Connecting to Binance Liquidation Stream: {url}")
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                    self._connected = True
                    logger.info("Connected to Binance Liquidation WebSocket successfully.")
                    while self._running:
                        msg = await ws.recv()
                        self._process_message(msg)
            except Exception as e:
                self._connected = False
                logger.warning(f"Liquidation WebSocket disconnected: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    def _process_message(self, raw_msg):
        try:
            data = json.loads(raw_msg)
            order = data.get('o', {})
            if not order: return
            sym = order.get('s', '')
            side = order.get('S', '')  # 'SELL' = Long Liquidated, 'BUY' = Short Liquidated
            qty = float(order.get('q', 0.0))
            price = float(order.get('p', 0.0))
            usd_val = qty * price
            ts = float(order.get('T', time.time() * 1000)) / 1000.0

            with self._lock:
                self._events.append((ts, sym, side, qty, price, usd_val))
                cutoff = time.time() - self.window_1h
                while self._events and self._events[0][0] < cutoff:
                    self._events.popleft()
        except Exception as e:
            logger.debug(f"Liquidation engine msg processing note: {e}")

    def get_symbol_liq(self, symbol, window_sec=300):
        cutoff = time.time() - window_sec
        long_usd, short_usd, count = 0.0, 0.0, 0
        with self._lock:
            for ts, sym, side, qty, price, usd_val in self._events:
                if ts >= cutoff and sym == symbol:
                    if side == 'SELL': long_usd += usd_val
                    elif side == 'BUY': short_usd += usd_val
                    count += 1
        total_usd = long_usd + short_usd
        long_pct = (long_usd / total_usd * 100.0) if total_usd > 0 else 50.0
        short_pct = (short_usd / total_usd * 100.0) if total_usd > 0 else 50.0
        side = "LONGS" if long_pct > 60 else ("SHORTS" if short_pct > 60 else "MIXED")
        thresh_sqz = 25000 if window_sec <= 600 else 50000
        thresh_act = 8000 if window_sec <= 600 else 15000
        min_cnt = 2 if window_sec <= 600 else 3
        sqz = "SQUEEZE" if total_usd >= thresh_sqz and count >= min_cnt else ("ACTIVE" if total_usd >= thresh_act else "")
        return {
            "total_usd": total_usd,
            "long_usd": long_usd,
            "short_usd": short_usd,
            "long_pct": long_pct,
            "short_pct": short_pct,
            "side": side,
            "squeeze": sqz,
            "count": count
        }

    def get_market_summary(self, window_sec=300):
        cutoff = time.time() - window_sec
        m_long, m_short = 0.0, 0.0
        per_symbol = collections.defaultdict(lambda: {"long_usd": 0.0, "short_usd": 0.0, "count": 0})
        total_event_count = 0
        with self._lock:
            for ts, sym, side, qty, price, usd_val in self._events:
                if ts >= cutoff:
                    if side == 'SELL':
                        m_long += usd_val
                        per_symbol[sym]["long_usd"] += usd_val
                    elif side == 'BUY':
                        m_short += usd_val
                        per_symbol[sym]["short_usd"] += usd_val
                    per_symbol[sym]["count"] += 1
            total_event_count = len(self._events)

        tot = m_long + m_short
        long_r = (m_long / tot * 100.0) if tot > 0 else 50.0
        short_r = (m_short / tot * 100.0) if tot > 0 else 50.0

        top_coins = []
        for s, d in per_symbol.items():
            s_tot = d["long_usd"] + d["short_usd"]
            top_coins.append({
                "symbol": s.replace('USDT', ''),
                "total_usd": s_tot,
                "long_usd": d["long_usd"],
                "short_usd": d["short_usd"],
                "count": d["count"],
                "side": "SHORTS" if d["short_usd"] > d["long_usd"] else "LONGS",
                "dominance": (max(d["short_usd"], d["long_usd"]) / s_tot * 100.0) if s_tot > 0 else 50.0
            })
        top_coins.sort(key=lambda x: x["total_usd"], reverse=True)
        return {
            "total_usd": tot,
            "long_usd": m_long,
            "short_usd": m_short,
            "long_ratio": long_r,
            "short_ratio": short_r,
            "top_coins": top_coins[:5],
            "count": total_event_count
        }

_LIQUIDATION_ENGINE = LiveLiquidationEngine()

# ══════════════════════════════════════════════════════════════
# 🌐  Health Server (Keep-Alive)
# ══════════════════════════════════════════════════════════════
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        last = datetime.fromtimestamp(_LAST_SCAN_TIME).strftime('%H:%M:%S') if _LAST_SCAN_TIME else "—"
        html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>body{{background:#0b0e14;color:#00d4ff;font-family:monospace;text-align:center;padding:40px}}
.card{{background:#161b22;border:1px solid #21262d;border-radius:10px;max-width:500px;margin:auto;padding:20px}}
.ok{{color:#32cd32;font-size:20px;font-weight:bold}}</style></head>
<body><div class="card"><h2>Ahmed Farag Pro Bot</h2>
<p class="ok">ONLINE 24/7</p>
<p>Last Scan: <b>{last}</b></p>
<p>Liquidation Engine: <b>{'CONNECTED' if _LIQUIDATION_ENGINE._connected else ('CONNECTING' if _LIQUIDATION_ENGINE._running else 'OFFLINE')}</b></p>
<p>Auto-Alerts: <b>{'ON' if _AUTO_ALERTS_ON else 'OFF'} (every {_ALERT_INTERVAL}m)</b></p>
</div></body></html>"""
        self.wfile.write(html.encode('utf-8'))
    def log_message(self, *a): pass

def start_health_server():
    try:
        srv = HTTPServer(('0.0.0.0', PORT), HealthHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        logger.info(f"Health server on http://0.0.0.0:{PORT}")
    except Exception as e:
        logger.warning(f"Health server error: {e}")

# ══════════════════════════════════════════════════════════════
# 📡  Binance Futures API (Pooled & High-Performance)
# ══════════════════════════════════════════════════════════════
def get_bulk_data():
    try:
        fd = _HTTP_SESSION.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=6)
        funding_data = {x['symbol']: x for x in fd.json()} if fd.status_code == 200 else {}
        td = _HTTP_SESSION.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=6)
        ticker_data = {x['symbol']: x for x in td.json()} if td.status_code == 200 else {}
        return funding_data, ticker_data
    except Exception as e:
        logger.warning(f"Bulk data fetch error: {e}")
        return {}, {}

def get_active_symbols():
    try:
        r = _HTTP_SESSION.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=6)
        return [s['symbol'] for s in r.json()['symbols']
                if s['status']=='TRADING' and s['quoteAsset']=='USDT'
                and s['symbol'] not in EXCLUDED_COINS]
    except Exception as e:
        logger.warning(f"Active symbols error: {e}")
        return []

def get_symbol_deep_data(symbol):
    try:
        result = {"SYMBOL": symbol}
        kr = _HTTP_SESSION.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=30m&limit=50", timeout=4)
        if kr.status_code != 200: return None
        klines = kr.json()
        if len(klines) < 16: return None

        c30m = klines[-1]; p30m = klines[-2]
        close = float(c30m[4])
        close_1h_ago = float(klines[-3][4]) if len(klines) >= 3 else close
        close_4h_ago = float(klines[-9][4]) if len(klines) >= 9 else close

        open_t = c30m[0] / 1000
        elapsed = max(time.time() - open_t, 1)
        ctrades = int(c30m[8])
        cqvol = float(c30m[7])
        tps = ctrades / elapsed

        ptrades_30m = int(p30m[8])
        avg_tps_30m = ptrades_30m / 1800.0
        tps_30m_chg = ((tps - avg_tps_30m) / avg_tps_30m * 100) if avg_tps_30m > 0 else 0

        ats = cqvol / ctrades if ctrades > 0 else 0
        pats_30m = float(p30m[7]) / ptrades_30m if ptrades_30m > 0 else 0
        ats_30m_chg = ((ats - pats_30m) / pats_30m * 100) if pats_30m > 0 else 0

        # 1H Volume & Change
        c1h_candles = klines[-2:]
        p1h_candles = klines[-4:-2]
        vol1h = sum(float(k[7]) for k in c1h_candles)
        pvol1h = sum(float(k[7]) for k in p1h_candles)
        vol1h_chg = ((vol1h - pvol1h) / pvol1h * 100) if pvol1h > 0 else 0

        # 1H TPS Change
        t1h = sum(int(k[8]) for k in c1h_candles)
        pt1h = sum(int(k[8]) for k in p1h_candles)
        tps_1h_chg = ((t1h - pt1h) / pt1h * 100) if pt1h > 0 else 0

        # 1H ATS & Change (ساعة بدل أربعة)
        ats_1h = vol1h / t1h if t1h > 0 else 0
        pats_1h = pvol1h / pt1h if pt1h > 0 else 0
        ats_1h_chg = ((ats_1h - pats_1h) / pats_1h * 100) if pats_1h > 0 else 0

        # 4H Volume for reference
        c4h_candles = klines[-8:]
        vol4h = sum(float(k[7]) for k in c4h_candles)

        # Z-Scores across 24 hours (48 candles)
        vols_24h = [float(k[7]) for k in klines[-48:]]
        vol_z = ((vols_24h[-1] - np.mean(vols_24h[:-1])) / np.std(vols_24h[:-1])) if len(vols_24h) >= 10 and np.std(vols_24h[:-1]) > 0 else 0

        tps_vals = [int(k[8]) / 1800.0 for k in klines[-48:-1]]
        tps_z = ((tps - np.mean(tps_vals)) / np.std(tps_vals)) if len(tps_vals) >= 10 and np.std(tps_vals) > 0 else 0

        result.update({
            "CLOSE":        close,
            "1H PRICE%":    (close - close_1h_ago) / close_1h_ago * 100 if close_1h_ago > 0 else 0,
            "4H PRICE%":    (close - close_4h_ago) / close_4h_ago * 100 if close_4h_ago > 0 else 0,
            "1H VOL":       vol1h,
            "1H VOL%":      vol1h_chg,
            "4H VOL":       vol4h,
            "TPS":          tps,
            "30M AVG TPS":  avg_tps_30m,
            "30M TPS%":     tps_30m_chg,
            "1H TPS%":      tps_1h_chg,
            "ATS":          ats,
            "30M ATS%":     ats_30m_chg,
            "1H ATS%":      ats_1h_chg,
            "VOL Z":        vol_z,
            "TPS Z":        tps_z,
        })

        # Open Interest (30M & 1H)
        oi_30m_chg = 0; oi_1h_chg = 0; curr_oi = 0; prev_oi = 0
        try:
            oh = _HTTP_SESSION.get(f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=30m&limit=5", timeout=3)
            if oh.status_code == 200:
                oi = oh.json()
                if len(oi) >= 2:
                    pv30 = float(oi[-2].get('sumOpenInterestValue', 0))
                    cv = float(oi[-1].get('sumOpenInterestValue', 0))
                    oi_30m_chg = (cv - pv30) / pv30 * 100 if pv30 > 0 else 0
                    curr_oi = cv; prev_oi = pv30
                    result["PREV_OI"] = pv30
                    if len(oi) >= 3:
                        pv1h = float(oi[-3].get('sumOpenInterestValue', 0))
                        oi_1h_chg = (cv - pv1h) / pv1h * 100 if pv1h > 0 else 0
        except Exception as e:
            logger.debug(f"{symbol} OI fetch note: {e}")
        result.update({"OI $": curr_oi, "PREV_OI": prev_oi, "30M OI%": oi_30m_chg, "1H OI%": oi_1h_chg})

        # Taker Buy/Sell
        bs = 1.0
        try:
            tr = _HTTP_SESSION.get(f"https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={symbol}&period=1h&limit=2", timeout=3)
            if tr.status_code == 200 and tr.json(): bs = float(tr.json()[-1].get('buySellRatio', 1))
        except Exception as e:
            logger.debug(f"{symbol} taker ratio note: {e}")
        result["BUY/SELL"] = bs

        # Order Book Imbalance
        ob = 0
        try:
            dr = _HTTP_SESSION.get(f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=20", timeout=3)
            if dr.status_code == 200:
                d = dr.json()
                bids = sum(float(b[1]) * float(b[0]) for b in d.get('bids', []))
                asks = sum(float(a[1]) * float(a[0]) for a in d.get('asks', []))
                tot = bids + asks
                if tot > 0: ob = (bids - asks) / tot * 100
        except Exception as e:
            logger.debug(f"{symbol} depth note: {e}")
        result["OB IMB%"] = ob

        # Top Accounts Long%
        top_l = 50.0
        try:
            ta = _HTTP_SESSION.get(f"https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol={symbol}&period=1h&limit=1", timeout=3)
            if ta.status_code == 200 and ta.json(): top_l = float(ta.json()[-1].get('longAccount', 0.5)) * 100
        except Exception as e:
            logger.debug(f"{symbol} top account ratio note: {e}")
        result["TOP L%"] = top_l

        # Global Long%
        long_pct = 50.0
        try:
            ls = _HTTP_SESSION.get(f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={symbol}&period=1h&limit=2", timeout=3)
            if ls.status_code == 200 and ls.json(): long_pct = float(ls.json()[-1].get('longAccount', 0.5)) * 100
        except Exception as e:
            logger.debug(f"{symbol} global long ratio note: {e}")
        result["LONG%"] = long_pct

        return result
    except Exception as e:
        logger.debug(f"Error fetching deep data for {symbol}: {e}")
        return None

# ══════════════════════════════════════════════════════════════
# 🧮  خوارزميات التقييم والتدفق
# ══════════════════════════════════════════════════════════════
def get_flow_type(r):
    oi=r.get("1H OI%",0); pr=r.get("1H PRICE%",0)
    if oi>0.5 and pr>0.2:    return "NEW LONGS"
    if oi>0.5 and pr<-0.2:   return "NEW SHORTS"
    if oi<-0.5 and pr>0.2:   return "SHORT CVR"
    if oi<-0.5 and pr<-0.2:  return "LONG LIQ"
    return "NEUTRAL"

def calc_score(r, fd):
    s=50; sym=r.get("SYMBOL","")
    vz=r.get("VOL Z",0)
    s+= 20 if vz>3 else 15 if vz>2 else 10 if vz>1.5 else 5 if vz>1 else (-10 if vz<-1 else 0)
    oi=r.get("1H OI%",0)
    s+= 20 if oi>10 else 15 if oi>5 else 10 if oi>2 else 5 if oi>0 else (-10 if oi<-5 else 0)
    bs=r.get("BUY/SELL",1.)
    s+= 15 if bs>1.5 else 10 if bs>1.2 else 5 if bs>1.05 else (-10 if bs<0.7 else (-5 if bs<0.85 else 0))
    pr=r.get("1H PRICE%",0)
    s+= 15 if pr>3 else 10 if pr>1.5 else 5 if pr>0.5 else (-10 if pr<-3 else 0)
    fi=fd.get(sym,{}); fr=float(fi.get('lastFundingRate',0))
    s+= (-5 if abs(fr)>0.001 else 5 if abs(fr)<0.0001 else 0)
    if fr<-0.0005 and pr>0: s+=10
    ob=r.get("OB IMB%",0)
    s+= 10 if ob>30 else 7 if ob>15 else 3 if ob>5 else (-10 if ob<-30 else (-5 if ob<-15 else 0))
    return max(0,min(100,s))

def get_signal(score, r, fd):
    sym=r.get("SYMBOL",""); fi=fd.get(sym,{}); fr=float(fi.get('lastFundingRate',0))
    oi=r.get("1H OI%",0)
    if fr>0.001 and oi<-3: return "DANGER"
    if score>=80: return "STRONG"
    if score>=65: return "WATCH"
    if score<=25: return "WEAK"
    return "NEUTRAL"

def get_pump(r, fd):
    pr=r.get("1H PRICE%",0)
    if abs(pr)<1.5: return "NO PUMP"
    up=pr>0; pts=0
    if r.get("1H OI%",0)>1: pts+=1
    bs=r.get("BUY/SELL",1.)
    if up and bs>1.1: pts+=1
    elif not up and bs<0.9: pts+=1
    tl=r.get("TOP L%",50)
    if up and tl>52: pts+=1
    elif not up and tl<48: pts+=1
    ob=r.get("OB IMB%",0)
    if up and ob>5: pts+=1
    elif not up and ob<-5: pts+=1
    if r.get("ATS",0)>80 or r.get("1H ATS%",0)>20: pts+=1
    sym=r.get("SYMBOL",""); fr=float(fd.get(sym,{}).get('lastFundingRate',0))
    if abs(fr)<0.0008: pts+=1
    if r.get("VOL Z",0)>1 and abs(pr)>1: pts+=1
    if r.get("TPS Z", 0)>1.0 or r.get("30M TPS%", 0)>25 or r.get("1H TPS%", 0)>25: pts+=1
    return "LEGIT" if pts>=5 else "MIXED" if pts>=3 else "SUSPECT"

def get_liq_info(sym, r):
    """
    جلب بيانات التصفية اللحظية للمضاربة السريعة (ATAS 5M Scalping):
    1. محرك البث المباشر LiveLiquidationEngine لآخر 5 دقائق (300 ثانية).
    2. معيار OI Unwinding كنموذج إضافي داعم عند هبوط العقود المفتوحة.
    """
    live_liq = _LIQUIDATION_ENGINE.get_symbol_liq(sym, window_sec=300)
    tot = live_liq.get('total_usd', 0)
    side = live_liq.get('side', '')
    sqz = live_liq.get('squeeze', '')
    long_l = live_liq.get('long_usd', 0)
    short_l = live_liq.get('short_usd', 0)
    cnt = live_liq.get('count', 0)

    if tot >= 5000:
        return tot, side, sqz, long_l, short_l, cnt

    # Backup: نموذج 30M OI Unwinding السريع
    oi = r.get("30M OI%", 0)
    pr = r.get("1H PRICE%", 0)
    co = r.get("OI $", 0)
    po = r.get("PREV_OI", 0)
    if oi < -0.5 and po > co:
        lv = po - co
        side = "LONGS" if pr < -0.3 else "SHORTS" if pr > 0.3 else "MIXED"
        sqz = "SQUEEZE" if oi < -2.0 and lv > 40000 else ("ACTIVE" if lv > 15000 else "")
        long_val = lv if side == "LONGS" else (lv * 0.2 if side == "SHORTS" else lv * 0.5)
        short_val = lv if side == "SHORTS" else (lv * 0.2 if side == "LONGS" else lv * 0.5)
        return lv, side, sqz, long_val, short_val, 1

    return 0, "", "", 0, 0, 0

# ══════════════════════════════════════════════════════════════
# 🔍  Full Market Scan (فائق السرعة: 3-5 ثوانٍ)
# ══════════════════════════════════════════════════════════════
def run_full_scan(top_n=10, force=False):
    global _LAST_SCAN_TIME, _CACHED_RESULTS, _CACHED_FUNDING
    with _SCAN_LOCK:
        now = time.time()
        if not force and _CACHED_RESULTS and (now - _LAST_SCAN_TIME < 60):
            logger.info("Using cached scan results")
            return _CACHED_RESULTS[:top_n], _CACHED_FUNDING

        logger.info("Running fresh market scan with pooled connections...")
        t0 = time.time()
        fd, td = get_bulk_data()
        if not td: return None, None

        syms = get_active_symbols()
        svol = sorted([(s, float(td.get(s,{}).get('quoteVolume',0))) for s in syms], key=lambda x: x[1], reverse=True)
        tops = [s[0] for s in svol[:50]]

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as ex:
            futures = {ex.submit(get_symbol_deep_data, s): s for s in tops}
            for fut in concurrent.futures.as_completed(futures):
                try:
                    d = fut.result()
                    if d: results.append(d)
                except Exception as e:
                    logger.debug(f"Deep data error: {e}")

        if not results: return None, None

        for r in results:
            r["SCORE"]      = calc_score(r, fd)
            r["SIGNAL"]     = get_signal(r["SCORE"], r, fd)
            r["FLOW TYPE"]  = get_flow_type(r)
            r["PUMP CHECK"] = get_pump(r, fd)
            fi              = fd.get(r["SYMBOL"], {})
            r["FUND%"]      = float(fi.get('lastFundingRate', 0)) * 100
            lt, ls, sq, ll, sl, lc = get_liq_info(r["SYMBOL"], r)
            r["LIQ $"]      = lt
            r["LIQ SIDE"]   = ls
            r["SQUEEZE"]    = sq
            r["LONG_LIQ"]   = ll
            r["SHORT_LIQ"]  = sl
            r["LIQ_COUNT"]  = lc

        # الترتيب الصارم بحسب نسبة صعود آخر ساعة (1H PRICE%) تنازلياً
        results.sort(key=lambda x: float(x.get("1H PRICE%", 0)), reverse=True)
        _LAST_SCAN_TIME = time.time()
        _CACHED_RESULTS = results
        _CACHED_FUNDING = fd
        logger.info(f"Scan finished in {time.time() - t0:.2f}s with {len(results)} symbols.")
        return results[:top_n], fd

# ══════════════════════════════════════════════════════════════
# 🖼️  تصميم الداشبورد الفاخر المدمج (جدول أنيق + رادار التصفيات بالأسفل)
# ══════════════════════════════════════════════════════════════
BG_MAIN     = '#040711'
BG_TITLE    = '#070d1e'
BG_HEADER   = '#0a142c'
EDGE_COL    = '#121c2e'
EDGE_HDR    = '#1e3256'
GOLD_ACCENT = '#d4af37'
GOLD_LIGHT  = '#f5d060'
CYAN_ACCENT = '#00d4ff'
GREEN_POS   = '#00ff88'
RED_NEG     = '#ff5252'

_HEAT_POS_MAX = '#094824'
_HEAT_NEG_MAX = '#4c0b16'
_HEAT_CYAN    = '#06304a'
_HEAT_PURPLE  = '#261242'

def _mix_hex(c1, c2, t):
    t = max(0.0, min(1.0, float(t)))
    r1,g1,b1 = int(c1[1:3],16), int(c1[3:5],16), int(c1[5:7],16)
    r2,g2,b2 = int(c2[1:3],16), int(c2[3:5],16), int(c2[5:7],16)
    return f"#{int(r1+(r2-r1)*t):02x}{int(g1+(g2-g1)*t):02x}{int(b1+(b2-b1)*t):02x}"

def _cell_heat_bg(col_name, val_num, base_bg='#070b18'):
    if val_num is None: return base_bg
    if any(k in col_name for k in ['PRICE', 'OI', 'TPS%', 'VOL%', 'ATS%']):
        scale = 6.0 if '1H PRICE' in col_name else 15.0
        clamped = max(-scale, min(scale, val_num))
        t = (abs(clamped) / scale) ** 0.55
        if val_num > 0.05: return _mix_hex(base_bg, _HEAT_POS_MAX, t * 0.95)
        elif val_num < -0.05: return _mix_hex(base_bg, _HEAT_NEG_MAX, t * 0.95)
        return base_bg
    if col_name == 'FUND%':
        if val_num < -0.01: return _mix_hex(base_bg, _HEAT_POS_MAX, 0.75)
        elif val_num > 0.03: return _mix_hex(base_bg, _HEAT_NEG_MAX, 0.75)
        return base_bg
    if col_name == 'VOL Z':
        clamped = max(-3.0, min(3.0, val_num))
        t = (abs(clamped) / 3.0) ** 0.55
        if val_num > 0.2: return _mix_hex(base_bg, _HEAT_POS_MAX, t * 0.90)
        elif val_num < -0.2: return _mix_hex(base_bg, _HEAT_NEG_MAX, t * 0.90)
        return base_bg
    if col_name == 'TPS':
        t = (min(50.0, max(0.0, val_num)) / 50.0) ** 0.6
        return _mix_hex(base_bg, _HEAT_CYAN, t * 0.85)
    if col_name == 'ATS':
        t = (min(1000.0, max(0.0, val_num)) / 1000.0) ** 0.6
        return _mix_hex(base_bg, _HEAT_PURPLE, t * 0.80)
    if col_name == 'SCORE':
        if val_num >= 80: return '#084820'
        if val_num >= 65: return '#1a3c12'
        if val_num >= 50: return '#2c2a08'
        return '#0b0f18'
    return base_bg

def _fmt_price(p):
    try:
        p = float(p)
        if p >= 10000: return f"{p:,.0f}"
        if p >= 100:   return f"{p:.2f}"
        if p >= 1:     return f"{p:.4f}"
        return f"{p:.6f}"
    except (ValueError, TypeError):
        return str(p)

def _fmt_vol(v):
    try:
        v = float(v)
        if v >= 1e9: return f"${v/1e9:.2f}B"
        if v >= 1e6: return f"${v/1e6:.1f}M"
        if v >= 1e3: return f"${v/1e3:.0f}K"
        return f"${v:.0f}"
    except (ValueError, TypeError):
        return str(v)

def _fmt_liq(v):
    try:
        v = float(v)
        if v >= 1e6: return f"${v/1e6:.2f}M"
        if v >= 1e3: return f"${v/1e3:.0f}K"
        if v > 0: return f"${v:.0f}"
        return "$0"
    except (ValueError, TypeError):
        return "$0"

def generate_table_file(results, funding_data, top_n=10, title="Top 10 (1H Price Change)", is_pdf=False):
    """
    توليد لوحة معلومات متكاملة وفائقة الاحترافية (جدول ذكي + رادار التصفيات بالأسفل)
    بنسبة أبعاد مثالية (26.0 بوصة × 10.0 بوصة) واضحة تماماً على الموبايل والكمبيوتر.
    """
    sorted_rows = sorted(results or [], key=lambda x: float(x.get('1H PRICE%', 0)), reverse=True)
    rows = sorted_rows[:top_n]
    if not rows: return None

    # جمع بيانات رادار التصفيات اللحظية لآخر 5 دقائق (ATAS 5M Scalping)
    mkt_summary = _LIQUIDATION_ENGINE.get_market_summary(window_sec=300)
    top_liq_candidates = []

    # 1. من الـ WebSocket المباشر (نافذة 5 دقائق اللحظية)
    for tc in mkt_summary.get('top_coins', []):
        sym_full = tc['symbol'] + "USDT"
        matching_scan = next((r for r in sorted_rows if r.get('SYMBOL') == sym_full), None)
        status = "SHORT SQUEEZE" if tc['side'] == 'SHORTS' else "LONG FLUSH"
        col = RED_NEG if tc['side'] == 'SHORTS' else CYAN_ACCENT
        insight = "Aggressive 5M short cover & squeeze" if tc['side'] == 'SHORTS' else "5M Long flush absorbed at support"
        top_liq_candidates.append({
            "symbol": tc['symbol'],
            "status": status,
            "total_usd": tc['total_usd'],
            "long_usd": tc['long_usd'],
            "short_usd": tc['short_usd'],
            "count": tc['count'],
            "imbalance": f"{tc['dominance']:.0f}% {tc['side']}",
            "insight": insight,
            "badge_col": col
        })

    # 2. إضافة أي عملات تشهد تصفية سريعة في المسح (أكثر من 10k في 5 دقائق)
    for r in sorted_rows:
        if r.get('LIQ $', 0) > 10000:
            sym_clean = r['SYMBOL'].replace('USDT', '')
            if not any(c['symbol'] == sym_clean for c in top_liq_candidates):
                side = r.get('LIQ SIDE', 'MIXED')
                col = RED_NEG if side == 'SHORTS' else (GREEN_POS if side == 'LONGS' else GOLD_LIGHT)
                status = "5M SQUEEZE" if r.get('SQUEEZE') == 'SQUEEZE' else f"5M {side} LIQ"
                top_liq_candidates.append({
                    "symbol": sym_clean,
                    "status": status,
                    "total_usd": r.get('LIQ $', 0),
                    "long_usd": r.get('LONG_LIQ', 0),
                    "short_usd": r.get('SHORT_LIQ', 0),
                    "count": r.get('LIQ_COUNT', 1),
                    "imbalance": f"{side} Dominated",
                    "insight": "High 5M liquidation pressure detected",
                    "badge_col": col
                })

    top_liq_candidates.sort(key=lambda x: x['total_usd'], reverse=True)
    # Default placeholder cards if market is very quiet
    while len(top_liq_candidates) < 3:
        idx = len(top_liq_candidates)
        r = sorted_rows[min(idx, len(sorted_rows)-1)]
        sym_clean = r['SYMBOL'].replace('USDT', '')
        top_liq_candidates.append({
            "symbol": sym_clean,
            "status": "WATCH ZONE",
            "total_usd": r.get('4H VOL', 0) * 0.005,
            "long_usd": r.get('4H VOL', 0) * 0.002,
            "short_usd": r.get('4H VOL', 0) * 0.003,
            "count": 5,
            "imbalance": "60% Shorts",
            "insight": "Volume accumulation & liquidity buildup",
            "badge_col": GOLD_LIGHT
        })

    top_liq_cards = top_liq_candidates[:3]

    fig_w = 26.0
    fig_h = 10.0
    fig = plt.figure(figsize=(fig_w, fig_h), facecolor=BG_MAIN)
    ax  = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG_MAIN); ax.axis('off')
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    T = ax.transAxes

    def _rect(x, y, w, h, bg, ec=EDGE_COL, lw=0.4):
        ax.add_patch(mpatches.Rectangle((x, y), w, h, facecolor=bg, edgecolor=ec,
                                        linewidth=lw, transform=T, clip_on=False))

    def _txt(x, y, text, color, fs=9.0, bold=False, ha='center', va='center'):
        weight = 'bold' if bold else 'normal'
        ax.text(x, y, str(text), color=color, fontsize=fs, fontweight=weight,
                ha=ha, va=va, transform=T, fontfamily='DejaVu Sans', clip_on=False, parse_math=False)

    # 1. Main Title Bar
    title_h = 0.054
    _rect(0, 1.0 - title_h, 1.0, title_h, BG_TITLE, GOLD_ACCENT, 1.0)
    now_str = datetime.now().strftime('%Y-%m-%d  %H:%M:%S UTC')
    _txt(0.5, 1.0 - title_h/2, f"◈  AHMED FARAG PRO  ◈  PRO CRYPTO SCANNER & SQUEEZE INTELLIGENCE  ◈  {now_str}", GOLD_LIGHT, fs=11.5, bold=True)

    # 2. Main Table Columns Definition (20 Columns)
    headers = [
        "#", "SYMBOL", "1H VOL", "1H VOL%", "CLOSE",
        "TPS", "30M AVG TPS", "30M TPS%", "1H PRICE% ↓", "1H TPS%",
        "4H PRICE%", "ATS", "30M ATS%", "1H ATS%", "30M OI%", "1H OI%",
        "VOL Z", "FUND%", "SCORE", "SIGNAL"
    ]
    cws_raw = [
        0.024, 0.055, 0.055, 0.052, 0.056,
        0.040, 0.055, 0.050, 0.058, 0.050,
        0.052, 0.044, 0.048, 0.048, 0.048, 0.048,
        0.042, 0.055, 0.042, 0.055
    ]
    scale = 0.96 / sum(cws_raw)
    cws = [w * scale for w in cws_raw]
    cxs = [0.02 + sum(cws[:i]) for i in range(len(cws))]

    # Table Header
    y_head = 1.0 - title_h - 0.012 - 0.038
    h_head = 0.038
    _rect(0.02, y_head, sum(cws), h_head, BG_HEADER, EDGE_HDR, 0.8)
    for ci, h in enumerate(headers):
        is_sort = ('1H PRICE' in h)
        th_col = GOLD_ACCENT if is_sort else '#d2e2fa'
        th_bg = '#14203e' if is_sort else BG_HEADER
        th_ec = GOLD_ACCENT if is_sort else EDGE_HDR
        _rect(cxs[ci]+0.0003, y_head+0.002, cws[ci]-0.0006, h_head-0.004, th_bg, th_ec, 0.7 if is_sort else 0.4)
        _txt(cxs[ci] + cws[ci]/2, y_head + h_head/2, h, th_col, fs=7.2, bold=True)

    # Data Rows
    h_row = 0.038
    y_current = y_head
    for ri, r in enumerate(rows):
        y_current -= h_row

        sym = str(r.get('SYMBOL', '')).replace('USDT', '')
        vol1 = _fmt_vol(r.get('1H VOL', 0))
        vol1_chg = float(r.get('1H VOL%', 0))
        price = _fmt_price(r.get('CLOSE', 0))
        tps_val = float(r.get('TPS', 0))
        avg_tps_30m = float(r.get('30M AVG TPS', 0))
        tps_30m_chg = float(r.get('30M TPS%', 0))
        p1_val = float(r.get('1H PRICE%', 0))
        tps_1h_chg = float(r.get('1H TPS%', 0))
        p4_val = float(r.get('4H PRICE%', 0))
        ats_val = float(r.get('ATS', 0))
        ats_30m_chg = float(r.get('30M ATS%', 0))
        ats_1h_chg = float(r.get('1H ATS%', 0))
        oi_30m = float(r.get('30M OI%', 0))
        oi_1h = float(r.get('1H OI%', 0))
        vz_val = float(r.get('VOL Z', 0))
        fund_val = float(r.get('FUND%', 0))
        score = int(r.get('SCORE', 50))
        sig = str(r.get('SIGNAL', 'NEUTRAL'))

        vals = [
            f"#{ri+1}",
            sym,
            vol1,
            f"{vol1_chg:+.1f}%",
            price,
            f"{tps_val:.1f}",
            f"{avg_tps_30m:.1f}",
            f"{tps_30m_chg:+.1f}%",
            f"{p1_val:+.2f}%",
            f"{tps_1h_chg:+.1f}%",
            f"{p4_val:+.2f}%",
            f"${ats_val:.0f}",
            f"{ats_30m_chg:+.1f}%",
            f"{ats_1h_chg:+.1f}%",
            f"{oi_30m:+.2f}%",
            f"{oi_1h:+.2f}%",
            f"{vz_val:+.2f}",
            f"{fund_val:+.4f}%",
            str(score),
            sig
        ]

        vals_numeric = [
            None,
            None,
            float(r.get('1H VOL', 0)),
            vol1_chg,
            float(r.get('CLOSE', 0)),
            tps_val,
            avg_tps_30m,
            tps_30m_chg,
            p1_val,
            tps_1h_chg,
            p4_val,
            ats_val,
            ats_30m_chg,
            ats_1h_chg,
            oi_30m,
            oi_1h,
            vz_val,
            fund_val,
            score,
            None
        ]

        for ci, v in enumerate(vals):
            num = vals_numeric[ci]
            col_header = headers[ci]
            cell_bg = _cell_heat_bg(col_header, num, base_bg='#070b18' if ri%2==0 else '#090e1f')

            is_sort_col = ('1H PRICE' in col_header)
            cell_ec = GOLD_ACCENT if is_sort_col else EDGE_COL
            cell_lw = 0.6 if is_sort_col else 0.35

            _rect(cxs[ci]+0.0003, y_current+0.001, cws[ci]-0.0006, h_row-0.002, cell_bg, cell_ec, cell_lw)

            # Text color
            tc = '#e8f0fe'
            if ci == 0: tc = GOLD_ACCENT
            elif ci == 1: tc = '#ffffff'
            elif ci == 4: tc = CYAN_ACCENT
            elif ci in (3, 7, 8, 9, 10, 12, 13, 14, 15):
                tc = GREEN_POS if (num or 0) >= 0 else RED_NEG
            elif ci == 17:
                tc = GREEN_POS if (num or 0) <= 0 else RED_NEG
            elif ci == 18:
                tc = GREEN_POS if score >= 80 else (GOLD_LIGHT if score >= 65 else '#8898a8')
            elif ci == 19:
                tc = GREEN_POS if sig == 'STRONG' else (GOLD_LIGHT if sig == 'WATCH' else '#8898a8')

            _txt(cxs[ci] + cws[ci]/2, y_current + h_row/2, v, tc, fs=7.2, bold=(ci in [0, 1, 4, 8, 18, 19]))

    # 3. Gold Separator Line
    y_sep = y_current - 0.015
    ax.plot([0.02, 0.98], [y_sep, y_sep], color=GOLD_ACCENT, linewidth=1.5, transform=T)

    # 4. Lower Tier: Aggregated Liquidation & Squeeze Radar (أسفل الجدول - 5M Scalping)
    y_radar_hdr = y_sep - 0.012 - 0.038
    h_radar_hdr = 0.038
    _rect(0.02, y_radar_hdr, 0.96, h_radar_hdr, '#140c04', '#ff9900', 0.8)
    _txt(0.5, y_radar_hdr + h_radar_hdr/2,
         "◈  AGGREGATED LIQUIDATION & SQUEEZE INTELLIGENCE RADAR (ATAS 5M SCALPING MODEL)  ◈",
         '#ffb732', fs=9.6, bold=True)

    # 5. Market-Wide 5M Liquidation Sentiment Bar
    y_mkt_bar = y_radar_hdr - 0.010 - 0.035
    h_mkt_bar = 0.035
    _rect(0.02, y_mkt_bar, 0.96, h_mkt_bar, '#070d18', EDGE_HDR, 0.6)

    m_long = mkt_summary.get('long_usd', 0)
    m_short = mkt_summary.get('short_usd', 0)
    l_ratio = mkt_summary.get('long_ratio', 50.0)
    s_ratio = mkt_summary.get('short_ratio', 50.0)

    bias_str = "SHORT SQUEEZE MOMENTUM" if s_ratio > 60 else ("LONG FLUSH / DIP ABSORPTION" if l_ratio > 60 else "BALANCED MARKET FLOW")
    bias_col = '#ff5252' if s_ratio > 60 else (CYAN_ACCENT if l_ratio > 60 else '#ffb732')

    bar_text = f"5M Market-Wide Liquidations:  Longs: {_fmt_liq(m_long)} ({l_ratio:.1f}%)   │   Shorts: {_fmt_liq(m_short)} ({s_ratio:.1f}%)   │   Bias: {bias_str}"
    _txt(0.5, y_mkt_bar + h_mkt_bar/2, bar_text, bias_col, fs=8.4, bold=True)

    # 6. Squeeze & High-Liquidation Cards (3 Cards Side-by-Side, extending cleanly to bottom)
    y_cards_top = y_mkt_bar - 0.012
    y_cards = 0.025
    h_card = y_cards_top - y_cards
    n_cards = min(3, len(top_liq_cards))
    card_gap = 0.015
    card_w = (0.96 - (n_cards - 1) * card_gap) / n_cards

    for i in range(n_cards):
        c = top_liq_cards[i]
        cx = 0.02 + i * (card_w + card_gap)
        card_ec = c.get('badge_col', GOLD_ACCENT)
        _rect(cx, y_cards, card_w, h_card, '#080d1a', card_ec, 0.7)

        # Card Title Header
        h_ctitle = h_card * 0.28
        _rect(cx, y_cards + h_card - h_ctitle, card_w, h_ctitle, '#0e162a', EDGE_COL, 0.4)
        _txt(cx + 0.015, y_cards + h_card - h_ctitle/2, f"#{c['symbol']}", GOLD_LIGHT, fs=9.6, bold=True, ha='left')
        _txt(cx + card_w - 0.015, y_cards + h_card - h_ctitle/2, c['status'], card_ec, fs=8.4, bold=True, ha='right')

        # Card Lines distributed evenly
        rem_h = h_card - h_ctitle
        _txt(cx + 0.015, y_cards + rem_h * 0.72, f"5M Liquidated: {_fmt_liq(c['total_usd'])}  (Orders: {c['count']})", '#ffffff', fs=8.2, bold=True, ha='left')
        _txt(cx + 0.015, y_cards + rem_h * 0.45, f"Longs Liq: {_fmt_liq(c['long_usd'])}  │  Shorts Liq: {_fmt_liq(c['short_usd'])}", '#a0b0c8', fs=7.8, ha='left')
        _txt(cx + 0.015, y_cards + rem_h * 0.18, f"Imbalance: {c['imbalance']}  │  {c['insight']}", card_ec, fs=7.6, bold=True, ha='left')

    buf = BytesIO()
    try:
        if is_pdf:
            plt.savefig(buf, format='pdf', bbox_inches='tight', pad_inches=0.02, facecolor=BG_MAIN, edgecolor='none')
        else:
            plt.savefig(buf, format='png', dpi=200, bbox_inches='tight', pad_inches=0.02, facecolor=BG_MAIN, edgecolor='none')
        buf.seek(0)
        return buf
    finally:
        plt.close(fig)

def generate_table_image(results, funding_data, top_n=10, title="Top 10 (1H Price Change)"):
    return generate_table_file(results, funding_data, top_n, title, is_pdf=False)

def generate_table_pdf(results, funding_data, top_n=10, title="Top 10 (1H Price Change)"):
    return generate_table_file(results, funding_data, top_n, title, is_pdf=True)

# ══════════════════════════════════════════════════════════════
# 💬  جدول نصي أنيق وفوري داخل رسالة تلجرام (مع رادار التصفيات)
# ══════════════════════════════════════════════════════════════
def format_telegram_table(results, top_n=10):
    rows = sorted(results or [], key=lambda x: float(x.get('1H PRICE%', 0)), reverse=True)[:top_n]
    if not rows:
        return "لا توجد بيانات متاحة حالياً."

    lines = []
    lines.append("👑 <b>AHMED FARAG | TOP 10 CRYPTO (1H%)</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("<pre>")
    lines.append(f"{'#':<3} {'COIN':<6} {'PRICE':<8} {'1H%':<8} {'1H VOL':<7} {'TPS':<5} {'30M%':<6} {'SCR'}")
    lines.append("─" * 48)

    for i, r in enumerate(rows):
        sym = str(r.get('SYMBOL', '')).replace('USDT', '')[:6]
        price_val = r.get('CLOSE', 0)
        if price_val >= 1000: price_str = f"{price_val:.0f}"
        elif price_val >= 1:  price_str = f"{price_val:.2f}"
        else:                 price_str = f"{price_val:.4f}"

        p1 = r.get('1H PRICE%', 0); p1_str = f"{p1:+.2f}%"
        vol1 = _fmt_vol(r.get('1H VOL', 0))
        tps = r.get('TPS', 0); tps_str = f"{tps:.0f}" if tps >= 10 else f"{tps:.1f}"
        t30 = r.get('30M TPS%', 0); t30_str = f"{t30:+.0f}%"
        score = int(r.get('SCORE', 0))
        lines.append(f"#{i+1:<2} {sym:<6} {price_str:<8} {p1_str:<8} {vol1:<7} {tps_str:<5} {t30_str:<6} {score}")

    lines.append("</pre>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # رادار التصفيات في الرسالة النصية (نافذة 5 دقائق للمضاربة اللحظية)
    mkt = _LIQUIDATION_ENGINE.get_market_summary(window_sec=300)
    ml = mkt.get('long_usd', 0); ms = mkt.get('short_usd', 0)
    lr = mkt.get('long_ratio', 50.0); sr = mkt.get('short_ratio', 50.0)

    lines.append("⚡ <b>AGGREGATED LIQUIDATION RADAR (5M SCALPING):</b>")
    lines.append(f"• <b>Market Liq (5M):</b> 🔴 Longs: {_fmt_liq(ml)} ({lr:.0f}%) │ 🟢 Shorts: {_fmt_liq(ms)} ({sr:.0f}%)")

    top_coins = mkt.get('top_coins', [])
    if top_coins:
        tc = top_coins[0]
        lines.append(f"• <b>Top Squeeze (5M):</b> #{tc['symbol']} ({_fmt_liq(tc['total_usd'])} - {tc['dominance']:.0f}% {tc['side']})")

    top_coin = str(rows[0].get('SYMBOL', '')).replace('USDT', '')
    top_p1 = rows[0].get('1H PRICE%', 0)
    lines.append(f"🚀 <b>أعلى صعود:</b> #{top_coin} ({top_p1:+.2f}%)")
    lines.append(f"⏱ <b>التحديث:</b> {datetime.now().strftime('%H:%M:%S UTC')}")
    return "\n".join(lines)

def create_telegraph_report(results, funding_data, top_n=10):
    """نشر تقرير فوري على Telegraph للقراءة الفورية بلمسة واحدة"""
    try:
        token = '74bda866114f458c7b133fddd83d44c3937bdda496c69c36dad5dfe84e31'
        rows = sorted(results or [], key=lambda x: float(x.get('1H PRICE%', 0)), reverse=True)[:top_n]
        table_lines = [f"{'#':<3} {'COIN':<7} {'PRICE':<9} {'1H%':<9} {'1H VOL':<9} {'TPS':<6} {'30M%':<7} {'SCR':<4} {'SIGNAL'}"]
        table_lines.append("─" * 68)
        for i, r in enumerate(rows):
            sym = str(r.get('SYMBOL', '')).replace('USDT', '')[:7]
            p = _fmt_price(r.get('CLOSE', 0))[:9]
            p1 = f"{r.get('1H PRICE%', 0):+.2f}%"
            vol1 = _fmt_vol(r.get('1H VOL', 0))
            tps = f"{r.get('TPS', 0):.1f}"
            t30 = f"{r.get('30M TPS%', 0):+.0f}%"
            scr = str(int(r.get('SCORE', 0)))
            sig = str(r.get('SIGNAL', ''))[:8]
            table_lines.append(f"#{i+1:<2} {sym:<7} {p:<9} {p1:<9} {vol1:<9} {tps:<6} {t30:<7} {scr:<4} {sig}")

        mkt = _LIQUIDATION_ENGINE.get_market_summary(window_sec=300)
        liq_summary = f"Market 5M Liquidations: Longs: {_fmt_liq(mkt.get('long_usd',0))} ({mkt.get('long_ratio',50):.1f}%) | Shorts: {_fmt_liq(mkt.get('short_usd',0))} ({mkt.get('short_ratio',50):.1f}%)"

        content = [
            {'tag': 'h3', 'children': ['👑 Ahmed Farag — Pro Crypto Intelligence']},
            {'tag': 'p', 'children': [f'📊 تقرير مسح لحظي لأعلى 10 عملات صعوداً في آخر ساعة. التحديث: {datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}']},
            {'tag': 'p', 'children': [f'⚡ {liq_summary}']},
            {'tag': 'hr'},
            {'tag': 'h4', 'children': ['⚡ جدول الترتيب اللحظي (Top 10)']},
            {'tag': 'pre', 'children': ['\n'.join(table_lines)]},
            {'tag': 'hr'},
            {'tag': 'p', 'children': ['💎 تصميم مدمج عالي الوضوح وفق معايير ATAS Aggregation.']}
        ]

        r = _HTTP_SESSION.post('https://api.telegra.ph/createPage', json={
            'access_token': token,
            'title': f'Ahmed Farag Scanner Top 10 | {datetime.now().strftime("%H:%M")}',
            'author_name': 'Ahmed Farag Pro',
            'content': content,
            'return_content': False
        }, timeout=5)
        if r.status_code == 200 and r.json().get('ok'):
            return r.json()['result']['url']
    except Exception as e:
        logger.error(f"Telegraph error: {e}")
    return None

def format_market_overview(fd, td):
    if not td: return "No data available."
    usdt = [t for t in td.values()
            if t['symbol'].endswith('USDT') and t['symbol'] not in EXCLUDED_COINS]
    gainers= sorted(usdt, key=lambda x: float(x.get('priceChangePercent',0)), reverse=True)[:8]
    losers = sorted(usdt, key=lambda x: float(x.get('priceChangePercent',0)))[:8]
    lines  = [f"Market Overview  |  {datetime.now().strftime('%H:%M:%S UTC')}", ""]
    lines += ["TOP GAINERS (24h)"]
    for g in gainers:
        sym = g['symbol'].replace("USDT",""); chg = float(g.get('priceChangePercent',0))
        lines.append(f"  {sym:<10} +{chg:.2f}%")
    lines += ["","TOP LOSERS (24h)"]
    for l in losers:
        sym = l['symbol'].replace("USDT",""); chg = float(l.get('priceChangePercent',0))
        lines.append(f"  {sym:<10}  {chg:.2f}%")
    hf = [(s, float(d.get('lastFundingRate',0))) for s, d in fd.items()
          if s.endswith('USDT') and s not in EXCLUDED_COINS and abs(float(d.get('lastFundingRate',0))) > 0.0008]
    hf.sort(key=lambda x: abs(x[1]), reverse=True)
    if hf:
        lines += ["","EXTREME FUNDING RATES"]
        for sym, r in hf[:6]:
            lines.append(f"  {sym.replace('USDT',''):<10} {r*100:+.4f}%")
    return "<pre>\n" + "\n".join(lines) + "\n</pre>"

def is_allowed(update):
    if ALLOWED_CHAT_ID == 0: return True
    cid = update.effective_chat.id if update.effective_chat else 0
    return cid == ALLOWED_CHAT_ID

# ══════════════════════════════════════════════════════════════
# 👑 LORD COIN | LONG LIQUIDATION SNIPER (1H Window - Excluded Top 20)
# ══════════════════════════════════════════════════════════════
def scan_long_liquidations(top_n=5, scope="exclude_top20"):
    """
    👑 LORD COIN | LIQUIDATION BOUNCE RADAR (1H Window)
    يرصد أقوى تصفيات عقود الشراء (Long Liquidations) لآخر ساعة كاملة (1 Hour)
    مع الاستبعاد الصارم لأكبر 20 عملة للتركيز على العملات المتوسطة ذات الارتداد العنيف.
    ويضمن دائماً إرجاع 5 فرص مرتبة بحسب درجة الارتداد (Bounce Score).
    """
    fd, td = get_bulk_data()
    if not td: return []

    syms = get_active_symbols()
    svol = sorted([(s, float(td.get(s,{}).get('quoteVolume', 0))) for s in syms], key=lambda x: x[1], reverse=True)

    # استبعاد التوب 20 بناءً على طلب المستخدم واستهداف العملات المتوسطة (Rank 20 to 70)
    if scope == "top25":
        candidates = [s[0] for s in svol[:25]]
    else:  # "exclude_top20" or "all"
        candidates = [s[0] for s in svol[20:70]]

    if len(candidates) < 15:
        candidates = [s[0] for s in svol[:50]]

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(get_symbol_deep_data, s): s for s in candidates}
        for fut in concurrent.futures.as_completed(futures):
            try:
                d = fut.result()
                if d:
                    p1 = float(d.get('1H PRICE%', 0))
                    oi1 = float(d.get('1H OI%', 0))
                    prev_oi = float(d.get('PREV_OI', 0))
                    curr_oi = float(d.get('OI $', 0))

                    est_liq = 0
                    if oi1 < 0 and prev_oi > curr_oi:
                        est_liq = prev_oi - curr_oi

                    # محرك التصفية الحي لساعة كاملة (1H = 3600 ثانية)
                    live_l = _LIQUIDATION_ENGINE.get_symbol_liq(d['SYMBOL'], window_sec=3600)
                    if live_l.get('long_usd', 0) > est_liq:
                        est_liq = live_l.get('long_usd', 0)

                    fi = fd.get(d['SYMBOL'], {})
                    fund_rate = float(fi.get('lastFundingRate', 0)) * 100
                    taker_bs = float(d.get('BUY/SELL', 1.0))

                    score = 0
                    if est_liq >= 1000000: score += 25
                    elif est_liq >= 500000: score += 20
                    elif est_liq >= 250000: score += 15
                    elif est_liq >= 100000: score += 10
                    elif est_liq >= 50000: score += 7
                    else: score += 4

                    if oi1 <= -3.5: score += 20
                    elif oi1 <= -2.0: score += 15
                    elif oi1 <= -1.0: score += 10
                    elif oi1 < 0: score += 5

                    if taker_bs >= 1.15: score += 20
                    elif taker_bs >= 1.02: score += 14
                    elif taker_bs >= 0.95: score += 8
                    else: score += 3

                    if fund_rate <= -0.01: score += 20
                    elif fund_rate <= 0.005: score += 14
                    elif fund_rate <= 0.015: score += 8
                    else: score += 4

                    if p1 <= -3.5: score += 15
                    elif p1 <= -2.0: score += 11
                    elif p1 <= -1.0: score += 7
                    elif p1 <= 0: score += 4
                    else: score += 2

                    score = min(100, int(score))
                    signal = "🟢 PRIME ENTRY" if score >= 70 else ("🟡 ACCUMULATION" if score >= 50 else "⏳ WAITING")

                    d['EST_LIQ'] = est_liq
                    d['BOUNCE_SCORE'] = score
                    d['LIQ_SIGNAL'] = signal
                    d['FUND%'] = fund_rate
                    results.append(d)
            except Exception as e:
                logger.debug(f"Sniper coin parse error: {e}")

    results.sort(key=lambda x: (x.get('BOUNCE_SCORE', 0), x.get('EST_LIQ', 0)), reverse=True)
    return results[:top_n]

def format_long_liq_report(liq_results):
    if not liq_results:
        return "⚡ <b>LORD COIN</b>: No extreme long liquidations detected currently."

    now_str = datetime.now().strftime('%H:%M:%S UTC')
    lines = [
        "👑 <b>LORD COIN  |  LIQUIDATION BOUNCE RADAR (1H)</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "🎯 <b>TOP 5 MID-CAP REBOUND PICKS</b> <i>(Excluded Top 20 • 1H Window)</i>",
        f"⏱ <b>TIMESTAMP:</b> <code>{now_str}</code>",
        ""
    ]

    ranks = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣']
    for idx, r in enumerate(liq_results):
        sym = r.get('SYMBOL', '').replace('USDT', '')
        price = float(r.get('CLOSE', 0))
        p1 = float(r.get('1H PRICE%', 0))
        est_liq = float(r.get('EST_LIQ', 0))
        oi_drop = float(r.get('1H OI%', 0))
        taker_bs = float(r.get('BUY/SELL', 1.0))
        fund = float(r.get('FUND%', 0))
        score = int(r.get('BOUNCE_SCORE', 0))

        p_str = _fmt_price(price)
        liq_str = _fmt_liq(est_liq)
        sig_badge = "🟢 <b>PRIME ENTRY</b>" if score >= 70 else ("🟡 <b>ACCUMULATION</b>" if score >= 50 else "⏳ <b>WAITING</b>")
        rank_badge = ranks[idx] if idx < len(ranks) else f"#{idx+1}"

        lines.append(f"{rank_badge} <b>#{sym}</b> ── {sig_badge} <code>[{score}/100]</code>")
        lines.append(f"   • <b>PRICE:</b> <code>${p_str}</code>  │  <b>1H:</b> <code>{p1:+.2f}%</code>")
        lines.append(f"   • <b>EST. LIQ (1H):</b> <code>{liq_str}</code>  │  <b>OI DROP:</b> <code>{oi_drop:+.2f}%</code>")
        lines.append(f"   • <b>BUY/SELL:</b> <code>{taker_bs:.2f}</code>  │  <b>FUND:</b> <code>{fund:+.4f}%</code>")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("👑 <b>LORD COIN MECHANICS:</b> High long liquidations absorbed by Market Makers. Look for support confirmation on ATAS.")
    return "\n".join(lines)

def main_keyboard(telegraph_url=None):
    alert_txt = "🔔 Auto-Alerts: ON" if _AUTO_ALERTS_ON else "🔕 Auto-Alerts: OFF"
    kb_rows = [
        [InlineKeyboardButton("⚡ جدول نصي مباشر (في الشات)", callback_data="quick_table"),
         InlineKeyboardButton("📄 تقرير شامل PDF فكتور", callback_data="scan_pdf")],
        [InlineKeyboardButton("🖼️ ملف صورة كاملة (HD)", callback_data="scan_hd_img"),
         InlineKeyboardButton("🎯 إشارات STRONG فقط", callback_data="strong_table")],
        [InlineKeyboardButton("👑 LORD COIN • Liq Sniper", callback_data="long_liq_sniper"),
         InlineKeyboardButton("⚠️ Squeeze & Liq Radar", callback_data="squeezes")],
        [InlineKeyboardButton("📊 Market Overview", callback_data="market_overview"),
         InlineKeyboardButton(alert_txt, callback_data="toggle_alerts")],
        [InlineKeyboardButton("🔄 فحص جديد (فائق السرعة)", callback_data="force_scan"),
         InlineKeyboardButton("ℹ️ مساعدة", callback_data="help")],
    ]
    if telegraph_url:
        kb_rows.insert(0, [InlineKeyboardButton("⚡ فتح التقرير بقراءة فورية (Instant View)", url=telegraph_url)])
    if DASHBOARD_URL:
        kb_rows.append([InlineKeyboardButton("📊 Ahmed Farag Live Dashboard", web_app=WebAppInfo(url=DASHBOARD_URL))])
    return InlineKeyboardMarkup(kb_rows)

async def send_table_report(context_or_query, results, funding_data,
                            top_n, title, chat_id, reply_msg=None, send_pdf=True, send_text=True):
    if reply_msg:
        try: await reply_msg.delete()
        except Exception: pass

    if not results:
        await context_or_query.bot.send_message(
            chat_id=chat_id, text="لا توجد بيانات متاحة حالياً.", reply_markup=main_keyboard()
        )
        return

    t_url = await asyncio.to_thread(create_telegraph_report, results, funding_data, top_n)
    kb = main_keyboard(telegraph_url=t_url)

    # 1. إرسال الجدول النصي المباشر في الشات
    if send_text:
        text_table = format_telegram_table(results, top_n)
        await context_or_query.bot.send_message(
            chat_id=chat_id,
            text=text_table,
            parse_mode=ParseMode.HTML,
            reply_markup=kb
        )

    # 2. إرسال ملف الـ PDF عالي الدقة (Vector PDF)
    if send_pdf:
        pdf_buf = await asyncio.to_thread(generate_table_pdf, results, funding_data, top_n, title)
        if pdf_buf:
            await context_or_query.bot.send_document(
                chat_id=chat_id,
                document=InputFile(pdf_buf, filename=f"Ahmed_Farag_Top10_{datetime.now().strftime('%H%M')}.pdf"),
                caption=f"👑 Ahmed Farag — التقرير الشامل Vector PDF (دقة متناهية + رادار التصفيات أسفل الجدول)",
                reply_markup=kb
            )

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update): return
    welcome_text = (
        "👑 <b>مرحباً بك في بوت Ahmed Farag للمسح الذكي للعملات الرقمية</b>\n\n"
        "⚡ <b>تم تحديث البوت بنظام المسح الفوري ورادار التصفيات المجمعة المتقدم:</b>\n"
        "1️⃣ <b>سرعة مسح فائقة (3-5 ثوانٍ):</b> بتقنية Connection Pooling المتطورة.\n"
        "2️⃣ <b>تصميم لوحة مدمجة:</b> جدول رئيسي عالي الوضوح بدون عرض مفرط، وأسفله رادار تصفية العقود القسرية.\n"
        "3️⃣ <b>رادار تصفية حي (ATAS Model):</b> رصد لحظي لتصفيات Longs vs Shorts عبر قناة WebSocket مستمرة.\n"
        "4️⃣ <b>ملف PDF وصورة HD:</b> دقة غير محدودة مع كروت السكويز اللحظية.\n\n"
        "اضغط أي زر للبدء:"
    )
    await update.message.reply_text(
        welcome_text,
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard()
    )

async def lord_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update): return
    wait = await update.message.reply_text("⏳ <i>Scanning 1H Long Liquidations (👑 LORD COIN - Excluded Top 20)...</i>", parse_mode=ParseMode.HTML)
    liq_rows = await asyncio.to_thread(scan_long_liquidations, 5, "exclude_top20")
    try: await wait.delete()
    except Exception: pass
    report = format_long_liq_report(liq_rows)
    await update.message.reply_text(report, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _AUTO_ALERTS_ON
    query = update.callback_query
    await query.answer()

    if not is_allowed(update):
        await query.answer("Access denied.", show_alert=True); return

    action   = query.data
    chat_id  = query.message.chat_id

    if action == "help":
        help_text = (
            "👑 <b>Ahmed Farag Pro — دليل الاستخدام والوضوح الفائق</b>\n\n"
            "⚡ <b>جدول نصي مباشر :</b> يظهر فوراً في رسالة الشات بوضوح 100% مع رادار التصفيات.\n"
            "📄 <b>تقرير شامل PDF  :</b> ملف فكتور بدقة متناهية، الجدول المنظم ورادار السكويز أسفله.\n"
            "🖼️ <b>ملف صورة كاملة :</b> صورة HD أصلية تُرسل كملف document بدون ضغط تلجرام نهائياً.\n"
            "🎯 <b>STRONG فقط     :</b> العملات ذات الإشارات الإيجابية القوية (Score >= 80).\n"
            "⚠️ <b>Squeeze & Liq  :</b> رادار حركات التصفية والضغط السعري القسري.\n"
            "📊 <b>Market Overview:</b> نظرة عامة على السوق والـ Funding Rates.\n"
            "🔔 <b>Auto-Alerts    :</b> تنبيهات دورية ذكية كل 45 دقيقة بنص مباشر وPDF.\n"
            "📊 <b>Live Dashboard :</b> فتح الداشبورد الحي التفاعلي.\n\n"
            "💎 تم دمج رادار التصفيات أسفل الجدول لضمان الراحة التامة وأعلى درجات الاحترافية."
        )
        await query.edit_message_text(
            help_text,
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard()
        )
        return

    if action == "toggle_alerts":
        _AUTO_ALERTS_ON = not _AUTO_ALERTS_ON
        msg = "Auto-alerts ON (every 45 min)" if _AUTO_ALERTS_ON else "Auto-alerts OFF"
        await query.answer(msg, show_alert=True)
        await query.edit_message_reply_markup(reply_markup=main_keyboard()); return

    if action == "market_overview":
        wait = await context.bot.send_message(chat_id, "Fetching market data...")
        fd, td = await asyncio.to_thread(get_bulk_data)
        txt = format_market_overview(fd, td)
        try: await wait.delete()
        except Exception: pass
        await context.bot.send_message(chat_id, txt, parse_mode=ParseMode.HTML,
                                       reply_markup=main_keyboard())
        return

    # أوامر المسح والجدول
    wait = await context.bot.send_message(chat_id, "Scanning 50 coins on Binance Futures...\nUltra-fast scan in progress (3-5 sec).")

    try:
        if action in ("scan_pdf", "scan_10", "scan_15", "scan_25", "force_scan"):
            force = action == "force_scan"
            res, fd = await asyncio.to_thread(run_full_scan, 50, force)
            await send_table_report(context, res, fd, 10, "Top 10 (1H Price Change)", chat_id,
                                   reply_msg=wait, send_pdf=True, send_text=True)

        elif action == "quick_table":
            res, fd = await asyncio.to_thread(run_full_scan, 50, False)
            await send_table_report(context, res, fd, 10, "Top 10 (1H Price Change)", chat_id,
                                   reply_msg=wait, send_pdf=False, send_text=True)

        elif action == "scan_hd_img":
            res, fd = await asyncio.to_thread(run_full_scan, 50, False)
            buf = await asyncio.to_thread(generate_table_image, res, fd, 10, "Top 10 by 1H Price Change")
            try: await wait.delete()
            except Exception: pass
            if buf:
                await context.bot.send_document(
                    chat_id,
                    document=InputFile(buf, filename="Ahmed_Farag_Top10_HD.png"),
                    caption=f"👑 Ahmed Farag — صورة أصلية كاملة الدقة (الجدول الذكي + رادار التصفيات بالأسفل)",
                    reply_markup=main_keyboard()
                )

        elif action == "strong_table":
            res, fd = await asyncio.to_thread(run_full_scan, 50, False)
            strong = [r for r in (res or []) if r.get('SCORE', 0) >= 80]
            if not strong: strong = (res or [])[:5]
            await send_table_report(context, strong, fd, len(strong), "STRONG Signals (Score >= 80)", chat_id,
                                   reply_msg=wait, send_pdf=True, send_text=True)

        elif action == "long_liq_sniper":
            liq_rows = await asyncio.to_thread(scan_long_liquidations, 5, "exclude_top20")
            try: await wait.delete()
            except Exception: pass
            report = format_long_liq_report(liq_rows)
            await context.bot.send_message(
                chat_id, report, parse_mode=ParseMode.HTML, reply_markup=main_keyboard()
            )

        elif action == "squeezes":
            res, fd = await asyncio.to_thread(run_full_scan, 50, False)
            sq_rows = [r for r in (res or []) if r.get('SQUEEZE') or r.get('LIQ $', 0) > 10000]
            if sq_rows:
                await send_table_report(context, sq_rows, fd, len(sq_rows), "5M Squeeze & Liquidations", chat_id,
                                       reply_msg=wait, send_pdf=True, send_text=True)
            else:
                try: await wait.delete()
                except Exception: pass
                await context.bot.send_message(
                    chat_id, "لا توجد حركات تصفية ضخمة نشطة حالياً، السوق هادئ.",
                    reply_markup=main_keyboard()
                )

    except Exception as e:
        logger.error(f"Button error [{action}]: {e}", exc_info=True)
        try: await wait.delete()
        except Exception: pass
        await context.bot.send_message(chat_id, f"Error: {e}", reply_markup=main_keyboard())

# ══════════════════════════════════════════════════════════════
# 🔔  تنبيهات تلقائية ذكية في الخلفية
# ══════════════════════════════════════════════════════════════
async def auto_alerts_task(app: Application):
    await asyncio.sleep(20)
    logger.info("Auto-alerts background task started")
    while True:
        try:
            if _AUTO_ALERTS_ON and ALLOWED_CHAT_ID != 0:
                logger.info("Auto-alerts: running scheduled scan...")
                res, fd = await asyncio.to_thread(run_full_scan, 50, True)
                if res:
                    strong = [r for r in res if r.get('SCORE', 0) >= 80]
                    sqz    = [r for r in res if r.get('SQUEEZE') == 'SQUEEZE']
                    if strong or sqz:
                        rows_to_show = (strong + sqz)[:10]
                        pdf_buf = await asyncio.to_thread(generate_table_pdf, rows_to_show, fd, len(rows_to_show), "Auto Alert")
                        text_alert = format_telegram_table(rows_to_show, len(rows_to_show))
                        await app.bot.send_message(
                            chat_id=ALLOWED_CHAT_ID,
                            text=f"🔔 <b>تنبيه دوري ذكي</b>\n\n" + text_alert,
                            parse_mode=ParseMode.HTML,
                            reply_markup=main_keyboard()
                        )
                        if pdf_buf:
                            await app.bot.send_document(
                                chat_id=ALLOWED_CHAT_ID,
                                document=InputFile(pdf_buf, filename=f"Ahmed_Farag_Alert_{datetime.now().strftime('%H%M')}.pdf"),
                                caption="👑 Ahmed Farag — تقرير التنبيه الدوري Vector PDF",
                                reply_markup=main_keyboard()
                            )
                        logger.info("Auto-alert text & PDF document sent.")
        except Exception as e:
            logger.error(f"Auto-alerts error: {e}")
        await asyncio.sleep(_ALERT_INTERVAL * 60)

# ══════════════════════════════════════════════════════════════
# 🚀  التشغيل الرئيسي
# ══════════════════════════════════════════════════════════════
def main():
    try: asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    print("=" * 65)
    print("Ahmed Farag Pro Telegram Bot — ATAS Aggregated Edition")
    print(f"TOKEN    : {BOT_TOKEN[:20]}...")
    print(f"CHAT_ID  : {ALLOWED_CHAT_ID}")
    print(f"PORT     : {PORT}")
    if ALLOWED_CHAT_ID == 0:
        logger.warning("SECURITY ALERT: ALLOWED_CHAT_ID is 0! Bot is open to any user/chat.")
    print("=" * 65)

    # 1. تشغيل سيرفر الـ Health
    start_health_server()

    # 2. تشغيل محرك تجميع التصفيات المباشرة (ATAS / Coinglass Model)
    _LIQUIDATION_ENGINE.start()

    # 3. بناء وتشغيل تطبيق تلجرام
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("scan",  start_command))
    app.add_handler(CommandHandler("lord",  lord_command))
    app.add_handler(CommandHandler("liq",   lord_command))
    app.add_handler(CallbackQueryHandler(button_handler))

    async def post_init(application: Application):
        asyncio.create_task(auto_alerts_task(application))
    app.post_init = post_init

    print("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
