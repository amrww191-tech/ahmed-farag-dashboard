import streamlit as st
import requests
import pandas as pd
import numpy as np
import time
import concurrent.futures
from datetime import datetime

# ---------------------------------------------------------
# إعدادات الصفحة
# ---------------------------------------------------------
st.set_page_config(page_title="MD Borsa Pro Scanner", layout="wide", page_icon="🔬")

st.markdown("""
<style>
    /* لون الخلفية الأساسي */
    body, .stApp { background-color: #0b0e14 !important; }
    
    /* تنسيق العنوان */
    .title-text {
        text-align: center;
        color: #00d4ff;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-size: 26px;
        font-weight: bold;
        margin-top: 10px;
        margin-bottom: 5px;
        text-shadow: 0 0 10px rgba(0, 212, 255, 0.3);
    }

    .subtitle-text {
        text-align: center;
        color: #6b7280;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-size: 12px;
        margin-bottom: 15px;
    }

    /* إجبار الجدول على التوسيط والخطوط */
    .stDataFrame td, .stDataFrame th {
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif !important;
        font-size: 11px !important;
        font-weight: bold !important;
        border: 1px solid #1e222d !important;
        padding: 6px 4px !important;
    }
    
    /* رأس الجدول */
    .stDataFrame th {
        background-color: #0c1445 !important; 
        color: white !important;
        text-transform: uppercase;
        text-align: center !important;
        font-size: 10px !important;
    }

    /* ميتريك كاردز */
    .metric-card {
        background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
        border: 1px solid #21262d;
        border-radius: 8px;
        padding: 12px 16px;
        text-align: center;
        margin-bottom: 10px;
    }
    .metric-value {
        font-size: 22px;
        font-weight: bold;
        color: #00d4ff;
    }
    .metric-label {
        font-size: 11px;
        color: #8b949e;
        margin-top: 4px;
    }
    .metric-green { color: #32cd32 !important; }
    .metric-red { color: #ff4c4c !important; }
    .metric-yellow { color: #ffa500 !important; }

    /* تنسيق التابات */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #161b22;
        border-radius: 6px;
        color: #8b949e;
        border: 1px solid #21262d;
    }
    .stTabs [aria-selected="true"] {
        background-color: #0c1445 !important;
        color: #00d4ff !important;
        border: 1px solid #00d4ff !important;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# العملات المستبعدة (الكبيرة جداً)
# ---------------------------------------------------------
EXCLUDED_COINS = [
    'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'USDCUSDT', 
    'ADAUSDT', 'DOGEUSDT', 'TRXUSDT', 'AVAXUSDT', 'LINKUSDT', 'MATICUSDT',
    'DOTUSDT', 'LTCUSDT', 'BCHUSDT', 'UNIUSDT', 'PEPEUSDT', 'SHIBUSDT'
]

# ---------------------------------------------------------
# المرحلة 1: جلب البيانات الأساسية (دفعة واحدة)
# ---------------------------------------------------------
@st.cache_data(ttl=55)
def get_bulk_data():
    """جلب Funding Rate و 24hr Ticker و Liquidations لكل العملات دفعة واحدة"""
    try:
        # Funding Rate + Mark Price لكل العملات
        funding_res = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=10)
        funding_data = {item['symbol']: item for item in funding_res.json()} if funding_res.status_code == 200 else {}
        
        # 24hr Ticker لكل العملات
        ticker_res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=10)
        ticker_data = {item['symbol']: item for item in ticker_res.json()} if ticker_res.status_code == 200 else {}
        
        # === جديد: جلب التصفيات (Liquidations) دفعة واحدة ===
        # نجلب آخر 1000 تصفية لكل العملات
        liq_data = {}
        try:
            liq_res = requests.get(
                "https://fapi.binance.com/fapi/v1/allForceOrders?limit=1000",
                timeout=10
            )
            if liq_res.status_code == 200:
                all_liqs = liq_res.json()
                # نحسب الوقت الحالي - ساعة واحدة (بالمللي ثانية)
                one_hour_ago = (time.time() - 3600) * 1000
                for liq in all_liqs:
                    sym = liq.get('symbol', '')
                    liq_time = liq.get('time', 0)
                    # نأخذ فقط التصفيات في آخر ساعة
                    if liq_time >= one_hour_ago:
                        if sym not in liq_data:
                            liq_data[sym] = {'buy_vol': 0.0, 'sell_vol': 0.0, 'count': 0}
                        
                        qty = float(liq.get('executedQty', 0))
                        price = float(liq.get('averagePrice', 0))
                        liq_value = qty * price
                        side = liq.get('side', '')
                        
                        # SELL = تصفية Longs (البورصة تبيع لإغلاق اللونج)
                        # BUY = تصفية Shorts (البورصة تشتري لإغلاق الشورت)
                        if side == 'SELL':
                            liq_data[sym]['sell_vol'] += liq_value  # Long liquidations
                        elif side == 'BUY':
                            liq_data[sym]['buy_vol'] += liq_value   # Short liquidations
                        liq_data[sym]['count'] += 1
        except Exception:
            pass
        
        return funding_data, ticker_data, liq_data
    except Exception:
        return {}, {}, {}

@st.cache_data(ttl=55)
def get_active_symbols():
    """جلب قائمة العملات النشطة مع التصفية"""
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        res = requests.get(url, timeout=8)
        symbols = [
            s['symbol'] for s in res.json()['symbols'] 
            if s['status'] == 'TRADING' 
            and s['quoteAsset'] == 'USDT' 
            and s['symbol'] not in EXCLUDED_COINS
        ]
        return symbols
    except Exception:
        return []

# ---------------------------------------------------------
# المرحلة 2: جلب البيانات التفصيلية لعملة واحدة
# ---------------------------------------------------------
def get_symbol_deep_data(symbol):
    """جلب كل البيانات التفصيلية لعملة واحدة"""
    try:
        result = {"SYMBOL": symbol}
        
        # === 1. Klines (25 شمعة 1h للإحصاءات) ===
        klines_res = requests.get(
            f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1h&limit=25",
            timeout=4
        )
        if klines_res.status_code != 200:
            return None
        klines = klines_res.json()
        if len(klines) < 9:
            return None

        current_1h = klines[-1]
        prev_1h = klines[-2]
        curr_4h_klines = klines[-4:]
        prev_4h_klines = klines[-8:-4]

        # --- حساب الفوليوم ---
        vol_4h = sum([float(k[7]) for k in curr_4h_klines])
        prev_vol_4h = sum([float(k[7]) for k in prev_4h_klines])
        vol_4h_chg = ((vol_4h - prev_vol_4h) / prev_vol_4h * 100) if prev_vol_4h > 0 else 0

        # --- حساب السعر ---
        close = float(current_1h[4])
        close_1h_ago = float(prev_1h[4])
        close_4h_ago = float(klines[-5][4])
        price_1h_chg = ((close - close_1h_ago) / close_1h_ago * 100) if close_1h_ago > 0 else 0
        price_4h_chg = ((close - close_4h_ago) / close_4h_ago * 100) if close_4h_ago > 0 else 0

        # --- حساب الـ TPS ---
        open_time = current_1h[0] / 1000
        seconds_elapsed = max(time.time() - open_time, 1)
        curr_trades = int(current_1h[8])
        tps_live = curr_trades / seconds_elapsed
        prev_1h_trades = int(prev_1h[8])
        avg_tps_1h = prev_1h_trades / 3600
        tps_1h_chg = ((tps_live - avg_tps_1h) / avg_tps_1h * 100) if avg_tps_1h > 0 else 0

        curr_4h_trades = sum([int(k[8]) for k in curr_4h_klines])
        prev_4h_trades = sum([int(k[8]) for k in prev_4h_klines])
        tps_4h_chg = ((curr_4h_trades - prev_4h_trades) / prev_4h_trades * 100) if prev_4h_trades > 0 else 0

        # --- حساب الـ ATS ---
        curr_quote_vol = float(current_1h[7])
        prev_quote_vol = float(prev_1h[7])
        ats_live = curr_quote_vol / curr_trades if curr_trades > 0 else 0
        ats_1h_ago = prev_quote_vol / prev_1h_trades if prev_1h_trades > 0 else 0
        ats_1h_chg = ((ats_live - ats_1h_ago) / ats_1h_ago * 100) if ats_1h_ago > 0 else 0

        # --- Volume Z-Score (آخر 24 شمعة) ---
        volumes_24h = [float(k[7]) for k in klines[-24:]]
        if len(volumes_24h) >= 10:
            vol_mean = np.mean(volumes_24h[:-1])
            vol_std = np.std(volumes_24h[:-1])
            current_vol = float(current_1h[7])
            vol_zscore = (current_vol - vol_mean) / vol_std if vol_std > 0 else 0
        else:
            vol_zscore = 0

        # --- TPS Z-Score ---
        trades_24h = [int(k[8]) for k in klines[-24:]]
        if len(trades_24h) >= 10:
            tps_values = [t / 3600 for t in trades_24h[:-1]]
            tps_mean = np.mean(tps_values)
            tps_std = np.std(tps_values)
            tps_zscore = (tps_live - tps_mean) / tps_std if tps_std > 0 else 0
        else:
            tps_zscore = 0

        result.update({
            "CLOSE": close,
            "1H PRICE%": price_1h_chg,
            "4H PRICE%": price_4h_chg,
            "4H VOL": vol_4h,
            "4H VOL%": vol_4h_chg,
            "TPS": tps_live,
            "1H TPS%": tps_1h_chg,
            "4H TPS%": tps_4h_chg,
            "ATS": ats_live,
            "1H ATS%": ats_1h_chg,
            "VOL Z": vol_zscore,
            "TPS Z": tps_zscore,
        })

        # === 2. Open Interest ===
        oi_chg = 0
        oi_4h_chg = 0
        curr_oi_val = 0
        try:
            oi_res = requests.get(
                f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}",
                timeout=3
            )
            if oi_res.status_code == 200:
                oi_hist_res = requests.get(
                    f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=1h&limit=5",
                    timeout=3
                )
                if oi_hist_res.status_code == 200:
                    oi_hist = oi_hist_res.json()
                    if len(oi_hist) >= 2:
                        prev_oi_val = float(oi_hist[-2].get('sumOpenInterestValue', 0))
                        curr_oi_val = float(oi_hist[-1].get('sumOpenInterestValue', 0))
                        oi_chg = ((curr_oi_val - prev_oi_val) / prev_oi_val * 100) if prev_oi_val > 0 else 0
                        result["PREV_OI"] = prev_oi_val
                        
                        if len(oi_hist) >= 5:
                            oi_4h_ago_val = float(oi_hist[0].get('sumOpenInterestValue', 0))
                            oi_4h_chg = ((curr_oi_val - oi_4h_ago_val) / oi_4h_ago_val * 100) if oi_4h_ago_val > 0 else 0
        except Exception:
            pass
            
        result["OI $"] = curr_oi_val
        result["1H OI%"] = oi_chg
        result["4H OI%"] = oi_4h_chg

        # === 3. Taker Buy/Sell Ratio ===
        buy_sell_ratio = 1.0
        try:
            taker_res = requests.get(
                f"https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={symbol}&period=1h&limit=5",
                timeout=3
            )
            if taker_res.status_code == 200:
                taker_data = taker_res.json()
                if taker_data:
                    latest_taker = taker_data[-1]
                    buy_sell_ratio = float(latest_taker.get('buySellRatio', 1))
        except Exception:
            pass
        
        result["BUY/SELL"] = buy_sell_ratio

        # === 4. Long/Short Ratio ===
        long_pct = 50.0
        ls_ratio = 1.0
        try:
            ls_res = requests.get(
                f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={symbol}&period=1h&limit=2",
                timeout=3
            )
            if ls_res.status_code == 200:
                ls_data = ls_res.json()
                if ls_data:
                    long_pct = float(ls_data[-1].get('longAccount', 0.5)) * 100
                    ls_ratio = float(ls_data[-1].get('longShortRatio', 1))
        except Exception:
            pass
        
        result["LONG%"] = long_pct
        result["L/S RATIO"] = ls_ratio

        # === 5. Order Book Imbalance ===
        ob_imbalance = 0
        try:
            depth_res = requests.get(
                f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=20",
                timeout=3
            )
            if depth_res.status_code == 200:
                depth = depth_res.json()
                total_bids = sum([float(b[1]) * float(b[0]) for b in depth.get('bids', [])])
                total_asks = sum([float(a[1]) * float(a[0]) for a in depth.get('asks', [])])
                total = total_bids + total_asks
                if total > 0:
                    ob_imbalance = ((total_bids - total_asks) / total) * 100
        except Exception:
            pass
        
        result["OB IMB%"] = ob_imbalance

        # === 6. جديد: Top Traders Positioning (Weight: 1 per call) ===
        top_long_account = 50.0
        top_long_position = 50.0
        try:
            top_acc_res = requests.get(
                f"https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol={symbol}&period=1h&limit=1",
                timeout=3
            )
            if top_acc_res.status_code == 200:
                top_acc_data = top_acc_res.json()
                if top_acc_data:
                    top_long_account = float(top_acc_data[-1].get('longAccount', 0.5)) * 100

            top_pos_res = requests.get(
                f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={symbol}&period=1h&limit=1",
                timeout=3
            )
            if top_pos_res.status_code == 200:
                top_pos_data = top_pos_res.json()
                if top_pos_data:
                    top_long_position = float(top_pos_data[-1].get('longPosition', 0.5)) * 100
        except Exception:
            pass
        
        result["TOP L%"] = top_long_account
        result["TOP POS%"] = top_long_position

        return result
    except Exception:
        return None


# ---------------------------------------------------------
# نظام OI + Price Matrix (اقتراح #1)
# ---------------------------------------------------------
def get_flow_type(row):
    """
    تصنيف نوع الحركة بناء على مصفوفة OI + Price.
    عتبات لتجنب الاشارات الوهمية عند التحركات الصغيرة.
    """
    oi_1h = row.get("1H OI%", 0)
    price_1h = row.get("1H PRICE%", 0)
    
    oi_threshold = 0.5
    price_threshold = 0.2
    
    oi_up = oi_1h > oi_threshold
    oi_down = oi_1h < -oi_threshold
    price_up = price_1h > price_threshold
    price_down = price_1h < -price_threshold
    
    if oi_up and price_up:
        return "🟢 NEW LONGS"
    elif oi_up and price_down:
        return "🔴 NEW SHORTS"
    elif oi_down and price_up:
        return "🟡 SHORT COVER"
    elif oi_down and price_down:
        return "⚪ LONG LIQ"
    else:
        return "➖ NEUTRAL"


# ---------------------------------------------------------
# نظام التصفيات (اقتراح #2)
# ---------------------------------------------------------
def get_liquidation_info(symbol, row, liq_data):
    """
    تحليل بيانات التصفيات لعملة محددة.
    يدعم البيانات المباشرة أو الحساب الرياضي الدقيق (Position Unwinding) من هبوط الـ OI.
    يعيد: (liq_total, liq_side, squeeze_status)
    """
    sym_liq = liq_data.get(symbol, {}) if liq_data else {}
    
    long_liq = sym_liq.get('sell_vol', 0)
    short_liq = sym_liq.get('buy_vol', 0)
    liq_total = long_liq + short_liq
    liq_count = sym_liq.get('count', 0)
    
    # 1. إذا كانت بيانات التصفيات المباشرة متوفرة
    if liq_total >= 100:
        if long_liq > short_liq * 1.5:
            liq_side = "🔴 LONGS"
        elif short_liq > long_liq * 1.5:
            liq_side = "🟢 SHORTS"
        else:
            liq_side = "⚖️ MIXED"
        
        squeeze_status = ""
        if liq_total > 50000 and liq_count >= 5:
            squeeze_status = "⚠️ SQUEEZE"
        elif liq_total > 20000 and liq_count >= 3:
            squeeze_status = "🔶 ACTIVE"
        
        return liq_total, liq_side, squeeze_status

    # 2. الحساب الرياضي الاحترافي (OI Unwinding Model) عند إغلاق/تصفية العقود
    oi_1h = row.get("1H OI%", 0)
    price_1h = row.get("1H PRICE%", 0)
    curr_oi = row.get("OI $", 0)
    prev_oi = row.get("PREV_OI", 0)
    
    if oi_1h < -1.5 and prev_oi > curr_oi:
        liq_val = prev_oi - curr_oi
        if price_1h < -0.8:
            liq_side = "🔴 LONGS"
        elif price_1h > 0.8:
            liq_side = "🟢 SHORTS"
        else:
            liq_side = "⚖️ MIXED"
            
        squeeze_status = ""
        if oi_1h < -4.0 and liq_val > 100000:
            squeeze_status = "⚠️ SQUEEZE"
        elif oi_1h < -2.0 and liq_val > 30000:
            squeeze_status = "🔶 ACTIVE"
            
        return liq_val, liq_side, squeeze_status
        
    return 0, "", ""


# ---------------------------------------------------------
# نظام Pump Legitimacy Score (اقتراح #6)
# ---------------------------------------------------------
def get_pump_legitimacy(row, funding_data):
    """
    مقياس شرعية البامب - 7 عوامل.
    LEGIT (5-7), MIXED (3-4), SUSPECT (0-2), NO PUMP (حركة < 1.5%)
    """
    price_1h = row.get("1H PRICE%", 0)
    
    if abs(price_1h) < 1.5:
        return "➖ NO PUMP"
    
    is_pump_up = price_1h > 0
    legit_points = 0
    
    # 1. OI يزيد (سيولة جديدة)
    oi_1h = row.get("1H OI%", 0)
    if oi_1h > 1.0:
        legit_points += 1
    
    # 2. Taker Buy/Sell يؤكد الاتجاه
    buy_sell = row.get("BUY/SELL", 1.0)
    if is_pump_up and buy_sell > 1.1:
        legit_points += 1
    elif not is_pump_up and buy_sell < 0.9:
        legit_points += 1
    
    # 3. كبار المتداولين متوافقين
    top_long = row.get("TOP POS%", 50)
    if is_pump_up and top_long > 52:
        legit_points += 1
    elif not is_pump_up and top_long < 48:
        legit_points += 1
    
    # 4. Order Book يدعم الاتجاه
    ob_imb = row.get("OB IMB%", 0)
    if is_pump_up and ob_imb > 5:
        legit_points += 1
    elif not is_pump_up and ob_imb < -5:
        legit_points += 1
    
    # 5. ATS عالي (صفقات كبيرة - مش wash trading)
    ats = row.get("ATS", 0)
    ats_1h_chg = row.get("1H ATS%", 0)
    if ats > 80 or ats_1h_chg > 20:
        legit_points += 1
    
    # 6. Funding Rate مش مبالغ فيه
    symbol = row.get("SYMBOL", "")
    fund_info = funding_data.get(symbol, {})
    funding_rate = float(fund_info.get('lastFundingRate', 0))
    if abs(funding_rate) < 0.0008:
        legit_points += 1
    
    # 7. فوليوم حقيقي + حركة سعرية متناسبة
    vol_z = row.get("VOL Z", 0)
    if vol_z > 1.0 and abs(price_1h) > 1.0:
        legit_points += 1
    
    if legit_points >= 5:
        return "🟢 LEGIT"
    elif legit_points >= 3:
        return "🟡 MIXED"
    else:
        return "🔴 SUSPECT"


# ---------------------------------------------------------
# نظام Smart Score
# ---------------------------------------------------------
def calculate_smart_score(row, funding_data):
    """حساب Smart Score من 0 إلى 100"""
    score = 50
    
    symbol = row.get("SYMBOL", "")
    
    # 1. Volume Z-Score (وزن 20 نقطة)
    vol_z = row.get("VOL Z", 0)
    if vol_z > 3.0:
        score += 20
    elif vol_z > 2.0:
        score += 15
    elif vol_z > 1.5:
        score += 10
    elif vol_z > 1.0:
        score += 5
    elif vol_z < -1.0:
        score -= 10
    
    # 2. OI Change (وزن 20 نقطة)
    oi_1h = row.get("1H OI%", 0)
    if oi_1h > 10:
        score += 20
    elif oi_1h > 5:
        score += 15
    elif oi_1h > 2:
        score += 10
    elif oi_1h > 0:
        score += 5
    elif oi_1h < -5:
        score -= 10
    
    # 3. Taker Buy Ratio (وزن 15 نقطة)
    buy_sell = row.get("BUY/SELL", 1.0)
    if buy_sell > 1.5:
        score += 15
    elif buy_sell > 1.2:
        score += 10
    elif buy_sell > 1.05:
        score += 5
    elif buy_sell < 0.7:
        score -= 10
    elif buy_sell < 0.85:
        score -= 5
    
    # 4. Price Momentum (وزن 15 نقطة)
    price_1h = row.get("1H PRICE%", 0)
    if price_1h > 3:
        score += 15
    elif price_1h > 1.5:
        score += 10
    elif price_1h > 0.5:
        score += 5
    elif price_1h < -3:
        score -= 10
    
    # 5. Funding Rate Signal (وزن 10 نقاط)
    funding_info = funding_data.get(symbol, {})
    funding_rate = float(funding_info.get('lastFundingRate', 0))
    if abs(funding_rate) > 0.001:
        score -= 5
    elif abs(funding_rate) < 0.0001:
        score += 5
    if funding_rate < -0.0005 and price_1h > 0:
        score += 10
    
    # 6. Order Book Imbalance (وزن 10 نقاط)
    ob_imb = row.get("OB IMB%", 0)
    if ob_imb > 30:
        score += 10
    elif ob_imb > 15:
        score += 7
    elif ob_imb > 5:
        score += 3
    elif ob_imb < -30:
        score -= 10
    elif ob_imb < -15:
        score -= 5
    
    return max(0, min(100, score))


# ---------------------------------------------------------
# نظام كشف البوتات
# ---------------------------------------------------------
def detect_bots(row):
    """كشف نشاط البوتات"""
    signals = []
    
    tps_z = row.get("TPS Z", 0)
    if tps_z > 3.0:
        signals.append("HIGH_FREQ")
    
    tps_1h_chg = row.get("1H TPS%", 0)
    vol_4h_chg = row.get("4H VOL%", 0)
    if tps_1h_chg > 100 and vol_4h_chg < 20:
        signals.append("WASH_SUSPECT")
    
    ats = row.get("ATS", 0)
    tps = row.get("TPS", 0)
    if tps > 5 and ats < 50:
        signals.append("MICRO_TRADES")
    
    vol_z = row.get("VOL Z", 0)
    price_1h = abs(row.get("1H PRICE%", 0))
    if vol_z > 2.5 and price_1h < 0.3:
        signals.append("VOL_NO_MOVE")
    
    if signals:
        return "🤖 " + ", ".join(signals)
    return ""


# ---------------------------------------------------------
# تحديد الـ Signal
# ---------------------------------------------------------
def get_signal(score, row, funding_data):
    """تحديد الإشارة بناء على Smart Score والبيانات"""
    symbol = row.get("SYMBOL", "")
    funding_info = funding_data.get(symbol, {})
    funding_rate = float(funding_info.get('lastFundingRate', 0))
    oi_1h = row.get("1H OI%", 0)
    
    if funding_rate > 0.001 and oi_1h < -3:
        return "🔴 DANGER"
    
    if score >= 80:
        return "🟢 STRONG"
    elif score >= 65:
        return "🟡 WATCH"
    elif score <= 25:
        return "🔴 WEAK"
    else:
        return "⚪ NEUTRAL"


# ---------------------------------------------------------
# تحديد دخول السيولة
# ---------------------------------------------------------
def get_liquidity_status(row):
    """هل دخلت سيولة جديدة؟"""
    oi_1h = row.get("1H OI%", 0)
    vol_z = row.get("VOL Z", 0)
    price_1h = row.get("1H PRICE%", 0)
    
    if oi_1h > 5 and vol_z > 1.5:
        if price_1h > 0:
            return "💰 INFLOW ↑"
        else:
            return "💰 INFLOW ↓"
    elif oi_1h < -5:
        return "🚪 OUTFLOW"
    elif oi_1h > 2:
        return "📈 BUILDING"
    else:
        return ""


# ---------------------------------------------------------
# تنسيق الألوان
# ---------------------------------------------------------
def style_scanner_table(df):
    """تنسيق الجدول بألوان احترافية"""
    
    def color_text(val):
        if isinstance(val, str):
            if '%' in val:
                try:
                    num = float(val.replace('%', '').replace(',', ''))
                    if num > 0: return 'color: #32cd32; font-weight: bold;'
                    elif num < 0: return 'color: #ff4c4c; font-weight: bold;'
                except Exception:
                    pass
            if '🟢' in val or 'STRONG' in val or 'LEGIT' in val: return 'color: #32cd32; font-weight: bold;'
            if '🔴' in val or 'DANGER' in val or 'WEAK' in val or 'SUSPECT' in val: return 'color: #ff4c4c; font-weight: bold;'
            if '🟡' in val or 'WATCH' in val or 'MIXED' in val or 'SHORT COVER' in val: return 'color: #ffa500; font-weight: bold;'
            if '🤖' in val: return 'color: #a855f7; font-weight: bold;'
            if '💰' in val or 'INFLOW' in val: return 'color: #00d4ff; font-weight: bold;'
            if '🚪' in val or 'OUTFLOW' in val: return 'color: #ff4c4c; font-weight: bold;'
            if '📈' in val: return 'color: #32cd32; font-weight: bold;'
            if '⚠️' in val or 'SQUEEZE' in val: return 'color: #ff6b6b; font-weight: bold;'
            if '🔶' in val or 'ACTIVE' in val: return 'color: #ffa500; font-weight: bold;'
            if '⚖️' in val: return 'color: #d1d4dc; font-weight: bold;'
            if '➖' in val: return 'color: #6b7280;'
            if '⚪' in val or 'LONG LIQ' in val: return 'color: #8b949e; font-weight: bold;'
        return 'color: #d1d4dc;'
    
    def get_bg_color(col_name):
        dark_blue = 'background-color: #081033;'
        black = 'background-color: #000000;'
        dark_green = 'background-color: #082b08;'
        dark_red = 'background-color: #2b0808;'
        dark_purple = 'background-color: #1a0033;'
        dark_orange = 'background-color: #332200;'
        dark_teal = 'background-color: #003333;'
        
        if col_name in ["SYMBOL", "SCORE", "SIGNAL"]:
            return dark_blue
        elif col_name in ["CLOSE", "1H PRICE%", "4H PRICE%"]:
            return black
        elif col_name in ["4H VOL", "4H VOL%", "VOL Z", "TPS", "1H TPS%", "ATS", "1H ATS%"]:
            return dark_blue
        elif col_name in ["1H OI%", "4H OI%", "LIQUIDITY", "FLOW TYPE"]:
            return dark_green
        elif col_name in ["BUY/SELL", "OB IMB%", "LONG%", "FUND%"]:
            return dark_red
        elif col_name in ["BOTS"]:
            return dark_purple
        elif col_name in ["LIQ $", "LIQ SIDE", "SQUEEZE"]:
            return dark_orange
        elif col_name in ["PUMP CHECK", "TOP L%"]:
            return dark_teal
        return dark_blue

    format_dict = {}
    for col in df.columns:
        if col in ["4H VOL", "OI $", "LIQ $"]:
            format_dict[col] = "{:,.0f}"
        elif col in ["CLOSE"]:
            format_dict[col] = "{:.6f}"
        elif col in ["TPS", "ATS", "BUY/SELL", "L/S RATIO", "VOL Z", "TPS Z"]:
            format_dict[col] = "{:.2f}"
        elif col in ["SCORE"]:
            format_dict[col] = "{:.0f}"
        elif "%" in col:
            format_dict[col] = "{:.2f}%"

    st_df = df.style.format(format_dict, na_rep="-")
    
    text_styled_cols = [
        "SIGNAL", "LIQUIDITY", "BOTS", "FLOW TYPE",
        "LIQ SIDE", "SQUEEZE", "PUMP CHECK"
    ]
    
    for col in df.columns:
        if col in format_dict or col in text_styled_cols:
            if hasattr(st_df, 'map'):
                st_df = st_df.map(lambda _, c=col: get_bg_color(c), subset=[col])
                st_df = st_df.map(color_text, subset=[col])
            else:
                st_df = st_df.applymap(lambda _, c=col: get_bg_color(c), subset=[col])
                st_df = st_df.applymap(color_text, subset=[col])
    
    # تلوين Score بتدرج
    if "SCORE" in df.columns:
        def score_color(val):
            try:
                num = float(val) if not isinstance(val, (int, float)) else val
                if num >= 80: return 'color: #32cd32; font-weight: bold; background-color: #082b08;'
                elif num >= 65: return 'color: #ffa500; font-weight: bold; background-color: #3d2e00;'
                elif num <= 25: return 'color: #ff4c4c; font-weight: bold; background-color: #3d0000;'
                else: return 'color: #d1d4dc; background-color: #081033;'
            except Exception:
                return 'color: #d1d4dc;'
        
        if hasattr(st_df, 'map'):
            st_df = st_df.map(score_color, subset=["SCORE"])
        else:
            st_df = st_df.applymap(score_color, subset=["SCORE"])
            
    st_df = st_df.set_properties(**{'text-align': 'center'})
    return st_df


# ---------------------------------------------------------
# الواجهة الرئيسية
# ---------------------------------------------------------
st.markdown("<div class='title-text'>🔬 MD Borsa Pro Scanner</div>", unsafe_allow_html=True)
st.markdown("<div class='subtitle-text'>Professional-Grade Crypto Futures Scanner | OI Flow · Liquidations · Pump Legitimacy · Order Flow · Smart Score</div>", unsafe_allow_html=True)

# التابات
tab1, tab2, tab3 = st.tabs(["🎯 Smart Scanner", "📊 Market Overview", "ℹ️ Guide"])

with tab1:
    col_btn1, col_btn2, col_sort, col_count, _ = st.columns([1, 1, 2, 1, 4])
    with col_btn1:
        update_clicked = st.button('🔄 Scan Now', use_container_width=True)
    with col_btn2:
        auto_refresh = st.checkbox('Auto (60s)', value=False)
    with col_sort:
        sort_by = st.selectbox('Sort by', [
            'SCORE', '1H PRICE%', '4H VOL%', 'VOL Z', '1H OI%', 'BUY/SELL', 'OB IMB%'
        ], index=0)
    with col_count:
        top_n = st.selectbox('Top', [15, 25, 35, 50], index=1)
    
    run_scan = update_clicked
    if auto_refresh:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=60000, limit=None, key="scanner_autorefresh")
            run_scan = True
        except ImportError:
            st.warning("⚠️ Install `streamlit-autorefresh` for auto-refresh: `pip install streamlit-autorefresh`")
    
    if run_scan:
        progress_bar = st.progress(0, text="🔍 Initializing scan...")
        
        progress_bar.progress(5, text="📡 Fetching bulk market data + liquidations...")
        funding_data, ticker_data, liq_data = get_bulk_data()
        
        if not ticker_data:
            progress_bar.empty()
            st.error("❌ Failed to fetch market data. Check your connection.")
        else:
            progress_bar.progress(15, text="🔢 Filtering top volume coins...")
            symbols = get_active_symbols()
            
            symbol_volumes = []
            for sym in symbols:
                ticker = ticker_data.get(sym, {})
                vol = float(ticker.get('quoteVolume', 0))
                symbol_volumes.append((sym, vol))
            
            symbol_volumes.sort(key=lambda x: x[1], reverse=True)
            top_symbols = [s[0] for s in symbol_volumes[:min(50, len(symbol_volumes))]]
            
            progress_bar.progress(25, text=f"📊 Deep scanning {len(top_symbols)} coins...")
            results = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures_map = {executor.submit(get_symbol_deep_data, sym): sym for sym in top_symbols}
                done_count = 0
                for future in concurrent.futures.as_completed(futures_map):
                    done_count += 1
                    pct = 25 + int((done_count / len(top_symbols)) * 55)
                    progress_bar.progress(min(pct, 80), text=f"📊 Scanning... {done_count}/{len(top_symbols)}")
                    data = future.result()
                    if data:
                        results.append(data)
            
            if results:
                progress_bar.progress(85, text="🧮 Calculating Smart Scores & Flow Analysis...")
                
                for r in results:
                    r["SCORE"] = calculate_smart_score(r, funding_data)
                    r["SIGNAL"] = get_signal(r["SCORE"], r, funding_data)
                    r["BOTS"] = detect_bots(r)
                    r["LIQUIDITY"] = get_liquidity_status(r)
                    
                    fund_info = funding_data.get(r["SYMBOL"], {})
                    r["FUND%"] = float(fund_info.get('lastFundingRate', 0)) * 100
                    
                    # اقتراح #1: OI + Price Matrix
                    r["FLOW TYPE"] = get_flow_type(r)
                    
                    # اقتراح #2: Liquidation Tracker
                    liq_total, liq_side, squeeze = get_liquidation_info(r["SYMBOL"], r, liq_data)
                    r["LIQ $"] = liq_total
                    r["LIQ SIDE"] = liq_side
                    r["SQUEEZE"] = squeeze
                    
                    # اقتراح #6: Pump Legitimacy Score
                    r["PUMP CHECK"] = get_pump_legitimacy(r, funding_data)
                
                progress_bar.progress(92, text="📋 Building display table...")
                
                df = pd.DataFrame(results)
                
                ascending = False
                if sort_by in df.columns:
                    df = df.sort_values(by=sort_by, ascending=ascending)
                
                df = df.head(top_n).reset_index(drop=True)
                
                display_cols = [
                    "SYMBOL", "SCORE", "SIGNAL", "PUMP CHECK", "FLOW TYPE",
                    "CLOSE", "1H PRICE%", "4H PRICE%",
                    "4H VOL", "4H VOL%", "VOL Z",
                    "ATS", "1H ATS%",
                    "1H OI%", "4H OI%", "FUND%",
                    "BUY/SELL", "OB IMB%", "TOP L%",
                    "LONG%", "TPS", "1H TPS%",
                    "LIQ $", "LIQ SIDE", "SQUEEZE",
                    "LIQUIDITY", "BOTS"
                ]
                display_cols = [c for c in display_cols if c in df.columns]
                df_display = df[display_cols]
                
                progress_bar.progress(100, text="✅ Scan complete!")
                time.sleep(0.5)
                progress_bar.empty()
                
                # --- عرض الكاردات الملخصة ---
                mc1, mc2, mc3, mc4, mc5, mc6, mc7, mc8 = st.columns(8)
                
                strong_count = len(df[df['SCORE'] >= 80])
                watch_count = len(df[(df['SCORE'] >= 65) & (df['SCORE'] < 80)])
                liquidity_in = len(df[df['LIQUIDITY'].str.contains('INFLOW', na=False)])
                bots_detected = len(df[df['BOTS'].str.len() > 0])
                avg_funding = df['FUND%'].mean() if 'FUND%' in df.columns else 0
                avg_score = df['SCORE'].mean()
                
                legit_pumps = len(df[df['PUMP CHECK'].str.contains('LEGIT', na=False)])
                suspect_pumps = len(df[df['PUMP CHECK'].str.contains('SUSPECT', na=False)])
                
                with mc1:
                    st.markdown(f"""<div class='metric-card'>
                        <div class='metric-value metric-green'>{strong_count}</div>
                        <div class='metric-label'>🟢 STRONG</div>
                    </div>""", unsafe_allow_html=True)
                with mc2:
                    st.markdown(f"""<div class='metric-card'>
                        <div class='metric-value metric-yellow'>{watch_count}</div>
                        <div class='metric-label'>🟡 WATCH</div>
                    </div>""", unsafe_allow_html=True)
                with mc3:
                    st.markdown(f"""<div class='metric-card'>
                        <div class='metric-value' style='color: #00d4ff;'>{liquidity_in}</div>
                        <div class='metric-label'>💰 INFLOW</div>
                    </div>""", unsafe_allow_html=True)
                with mc4:
                    st.markdown(f"""<div class='metric-card'>
                        <div class='metric-value metric-green'>{legit_pumps}</div>
                        <div class='metric-label'>✅ LEGIT PUMPS</div>
                    </div>""", unsafe_allow_html=True)
                with mc5:
                    st.markdown(f"""<div class='metric-card'>
                        <div class='metric-value metric-red'>{suspect_pumps}</div>
                        <div class='metric-label'>🔴 SUSPECT</div>
                    </div>""", unsafe_allow_html=True)
                with mc6:
                    color_class = 'metric-red' if bots_detected > 3 else 'metric-value'
                    st.markdown(f"""<div class='metric-card'>
                        <div class='{color_class}' style='font-size:22px; font-weight:bold;'>{bots_detected}</div>
                        <div class='metric-label'>🤖 BOTS</div>
                    </div>""", unsafe_allow_html=True)
                with mc7:
                    fund_color = 'metric-red' if abs(avg_funding) > 0.01 else 'metric-green'
                    st.markdown(f"""<div class='metric-card'>
                        <div class='metric-value {fund_color}'>{avg_funding:.4f}%</div>
                        <div class='metric-label'>📊 AVG FUND</div>
                    </div>""", unsafe_allow_html=True)
                with mc8:
                    score_color = 'metric-green' if avg_score > 55 else 'metric-red'
                    st.markdown(f"""<div class='metric-card'>
                        <div class='metric-value {score_color}'>{avg_score:.0f}</div>
                        <div class='metric-label'>📈 AVG SCORE</div>
                    </div>""", unsafe_allow_html=True)
                
                # --- الجدول الرئيسي ---
                st.markdown("---")
                styled_df = style_scanner_table(df_display)
                st.dataframe(styled_df, use_container_width=True, height=600, hide_index=True)
                
                # --- شرح الإشارات ---
                with st.expander("📖 Signal Legend"):
                    leg1, leg2, leg3, leg4 = st.columns(4)
                    with leg1:
                        st.markdown("""
                        **Signals:**
                        - 🟢 **STRONG** = Score ≥ 80
                        - 🟡 **WATCH** = Score 65-79
                        - ⚪ **NEUTRAL** = Score 26-64
                        - 🔴 **WEAK/DANGER** = Score ≤ 25
                        """)
                    with leg2:
                        st.markdown("""
                        **Flow Type (OI+Price):**
                        - 🟢 **NEW LONGS** = OI↑ + Price↑
                        - 🔴 **NEW SHORTS** = OI↑ + Price↓
                        - 🟡 **SHORT COVER** = OI↓ + Price↑
                        - ⚪ **LONG LIQ** = OI↓ + Price↓
                        """)
                    with leg3:
                        st.markdown("""
                        **Pump Check:**
                        - 🟢 **LEGIT** = بامب حقيقي (5+ عوامل)
                        - 🟡 **MIXED** = مختلط (3-4 عوامل)
                        - 🔴 **SUSPECT** = مشبوه (0-2 عوامل)
                        - ➖ **NO PUMP** = لا يوجد حركة
                        """)
                    with leg4:
                        st.markdown("""
                        **Liquidations:**
                        - ⚠️ **SQUEEZE** = تصفيات ضخمة
                        - 🔶 **ACTIVE** = تصفيات نشطة
                        - 🔴 **LONGS** = تصفية مشترين
                        - 🟢 **SHORTS** = تصفية بائعين
                        """)
                
                st.markdown(f"<div style='text-align: center; color: #6b7280; font-size: 11px; margin-top: 10px;'>Last scan: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Scanned {len(top_symbols)} coins | Showing top {top_n} by {sort_by}</div>", unsafe_allow_html=True)
            else:
                progress_bar.empty()
                st.error("❌ No data returned. Check your internet connection.")

with tab2:
    st.markdown("### 📊 Market Overview")
    
    if st.button("📊 Load Market Stats"):
        with st.spinner("Loading market overview..."):
            funding_data_ov, ticker_data_ov, _ = get_bulk_data()
            
            if ticker_data_ov:
                usdt_tickers = [
                    t for t in ticker_data_ov.values() 
                    if t['symbol'].endswith('USDT') and t['symbol'] not in EXCLUDED_COINS
                ]
                
                gainers = sorted(usdt_tickers, key=lambda x: float(x.get('priceChangePercent', 0)), reverse=True)[:10]
                losers = sorted(usdt_tickers, key=lambda x: float(x.get('priceChangePercent', 0)))[:10]
                vol_leaders = sorted(usdt_tickers, key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)[:10]
                
                g1, g2, g3 = st.columns(3)
                
                with g1:
                    st.markdown("#### 🟢 Top 10 Gainers (24h)")
                    gainer_data = [{
                        "Symbol": g['symbol'],
                        "Price": float(g['lastPrice']),
                        "Change%": f"{float(g['priceChangePercent']):.2f}%",
                        "Volume $": f"{float(g['quoteVolume']):,.0f}"
                    } for g in gainers]
                    st.dataframe(pd.DataFrame(gainer_data), hide_index=True, use_container_width=True)
                
                with g2:
                    st.markdown("#### 🔴 Top 10 Losers (24h)")
                    loser_data = [{
                        "Symbol": l['symbol'],
                        "Price": float(l['lastPrice']),
                        "Change%": f"{float(l['priceChangePercent']):.2f}%",
                        "Volume $": f"{float(l['quoteVolume']):,.0f}"
                    } for l in losers]
                    st.dataframe(pd.DataFrame(loser_data), hide_index=True, use_container_width=True)
                
                with g3:
                    st.markdown("#### 📊 Top 10 Volume (24h)")
                    vol_data = [{
                        "Symbol": v['symbol'],
                        "Volume $": f"{float(v['quoteVolume']):,.0f}",
                        "Change%": f"{float(v['priceChangePercent']):.2f}%"
                    } for v in vol_leaders]
                    st.dataframe(pd.DataFrame(vol_data), hide_index=True, use_container_width=True)
                
                st.markdown("---")
                st.markdown("#### ⚡ Extreme Funding Rates")
                
                fund_items = []
                for sym, fdata in funding_data_ov.items():
                    if sym.endswith('USDT') and sym not in EXCLUDED_COINS:
                        rate = float(fdata.get('lastFundingRate', 0))
                        if abs(rate) > 0.0005:
                            status = "🔴 OVER-LEVERAGED LONG" if rate > 0.001 else ("🟢 OVER-LEVERAGED SHORT" if rate < -0.001 else "🟡 ELEVATED")
                            fund_items.append({
                                "Symbol": sym,
                                "Funding Rate": f"{rate * 100:.4f}%",
                                "Mark Price": float(fdata.get('markPrice', 0)),
                                "Status": status
                            })
                
                if fund_items:
                    fund_items.sort(key=lambda x: abs(float(x['Funding Rate'].replace('%', ''))), reverse=True)
                    st.dataframe(pd.DataFrame(fund_items[:15]), hide_index=True, use_container_width=True)
                else:
                    st.info("No extreme funding rates detected currently.")

with tab3:
    st.markdown("""
    ### 📖 MD Borsa Pro Scanner - دليل الاستخدام

    ---

    #### 🎯 Smart Score (0-100)
    تقييم ذكي متعدد العوامل لكل عملة:
    
    | العامل | الوزن | الوصف |
    |:---|:---|:---|
    | **Volume Z-Score** | 22% | هل الفوليوم غير طبيعي إحصائياً؟ |
    | **OI Change** | 22% | هل دخلت سيولة جديدة؟ |
    | **Taker Buy Ratio** | 17% | هل الشراء عدواني؟ |
    | **Price Momentum** | 17% | اتجاه وقوة حركة السعر |
    | **Funding Rate** | 11% | هل السوق مزدحم في اتجاه واحد؟ |
    | **Order Book Imbalance** | 11% | هل طلبات الشراء أكثر من البيع؟ |

    ---

    #### 🆕 FLOW TYPE — مصفوفة OI + السعر
    أهم مؤشر لفهم **سبب** الحركة:
    
    | الحالة | المعنى | التفسير |
    |:---|:---|:---|
    | 🟢 **NEW LONGS** | OI↑ + Price↑ | سيولة جديدة تشتري — أقوى إشارة صعود |
    | 🔴 **NEW SHORTS** | OI↑ + Price↓ | سيولة جديدة تبيع — أقوى إشارة هبوط |
    | 🟡 **SHORT COVER** | OI↓ + Price↑ | تغطية شورت — صعود مؤقت وضعيف |
    | ⚪ **LONG LIQ** | OI↓ + Price↓ | تصفية لونج — كابيتيوليشن |
    | ➖ **NEUTRAL** | ثابت | لا توجد حركة واضحة |

    ---

    #### ⚠️ SQUEEZE & Liquidations
    
    | العمود | الوصف |
    |:---|:---|
    | **LIQ $** | إجمالي حجم التصفيات بالدولار في آخر ساعة |
    | **LIQ SIDE** | 🔴 LONGS = تصفية مشترين / 🟢 SHORTS = تصفية بائعين |
    | **SQUEEZE** | ⚠️ SQUEEZE = تصفيات ضخمة (> $50K) / 🔶 ACTIVE = نشطة |

    ---

    #### ✅ PUMP CHECK — مقياس شرعية البامب
    يفحص **7 عوامل** لتحديد هل الحركة حقيقية أم تلاعب:
    
    | العامل | بامب حقيقي ✅ | تلاعب ❌ |
    |:---|:---|:---|
    | **OI** | يزيد (سيولة جديدة) | ثابت أو ينخفض |
    | **Buy/Sell** | يؤكد الاتجاه | يعاكس الاتجاه |
    | **Top Traders** | يتداولوا مع الاتجاه | يتداولوا ضد الاتجاه |
    | **Order Book** | يدعم الاتجاه | يعاكس الاتجاه |
    | **ATS** | صفقات كبيرة | صفقات صغيرة وهمية |
    | **Funding** | طبيعي (< 0.08%) | مبالغ فيه |
    | **Volume** | حقيقي مع حركة | بدون تأثير على السعر |
    
    **النتيجة:**
    - 🟢 **LEGIT** (5-7/7) = بامب حقيقي
    - 🟡 **MIXED** (3-4/7) = مختلط
    - 🔴 **SUSPECT** (0-2/7) = مشبوه

    ---

    #### 📊 شرح باقي الأعمدة
    
    | العمود | المصدر | الوصف |
    |:---|:---|:---|
    | **4H VOL** | Klines | حجم تداول العملة بالدولار في آخر 4 ساعات |
    | **VOL Z** | Z-Score | كم انحراف معياري الفوليوم فوق المتوسط |
    | **ATS** | Klines | متوسط حجم الصفقة الواحدة بالدولار |
    | **1H OI%** | Open Interest | تغير المراكز المفتوحة |
    | **FUND%** | Funding Rate | معدل التمويل |
    | **BUY/SELL** | Taker Ratio | نسبة حجم الشراء/البيع |
    | **OB IMB%** | Order Book | اختلال توازن الطلبات |
    | **TOP L%** | Top Traders | نسبة كبار المتداولين Long |
    | **LONG%** | L/S Ratio | نسبة الحسابات Long (كل المتداولين) |

    ---

    #### 💡 كيف تقرأ البيانات كمحترف؟
    
    **سيناريو 1: بامب حقيقي 🟢**
    - FLOW TYPE = NEW LONGS + PUMP CHECK = LEGIT + VOL Z > 2

    **سيناريو 2: تصريف / فخ 🔴**
    - FLOW TYPE = SHORT COVER + PUMP CHECK = SUSPECT + FUND% عالي

    **سيناريو 3: Squeeze خطير ⚠️**
    - SQUEEZE = ⚠️ + LIQ SIDE = LONGS + OI% سلبي

    ---

    #### ⚠️ تحذيرات مهمة
    
    - **Score > 80 لا يعني اشتري!** - هو مؤشر على نشاط غير عادي
    - **PUMP CHECK = LEGIT لا يعني الدخول بدون تحليل!**
    - **Funding عالي = خطر** - حتى لو Score عالي
    - **استخدم مع أدوات أخرى** - لا تعتمد على أداة واحدة
    
    ---
    
    #### 🔗 Data Sources
    All data from **Binance Futures API** (free, no API key needed):
    - `/fapi/v1/klines` - Price & Volume candles
    - `/fapi/v1/premiumIndex` - Funding Rate & Mark Price
    - `/fapi/v1/openInterest` - Open Interest
    - `/futures/data/openInterestHist` - OI History
    - `/futures/data/takerlongshortRatio` - Taker Buy/Sell Volume
    - `/futures/data/globalLongShortAccountRatio` - Long/Short Ratio
    - `/futures/data/topLongShortAccountRatio` - Top Traders Account Ratio 🆕
    - `/futures/data/topLongShortPositionRatio` - Top Traders Position Ratio 🆕
    - `/fapi/v1/allForceOrders` - Liquidation Orders 🆕
    - `/fapi/v1/depth` - Order Book (Top 20 levels)
    - `/fapi/v1/ticker/24hr` - 24hr Stats
    """)
