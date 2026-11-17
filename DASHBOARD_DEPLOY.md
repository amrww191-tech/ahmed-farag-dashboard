# 📊 Ahmed Farag Dashboard — دليل النشر المجاني

## الملفات المُنشأة

| الملف | الوصف |
|-------|--------|
| `dashboard.html` | الداشبورد الكامل — جاهز للنشر |
| `telegram_bot.py` | محدَّث بزر الداشبورد في الكيبورد |

---

## خطوات النشر على GitHub Pages (مجاناً)

### الخطوة 1 — إنشاء حساب GitHub
1. اذهب إلى [github.com](https://github.com)
2. اضغط **Sign up** وأنشئ حساباً (مجاني)

### الخطوة 2 — إنشاء Repository جديدة
1. اضغط **+** ← **New repository**
2. اسم الـ Repo: `ahmed-farag-dashboard`
3. اجعلها **Public**
4. ✅ اختر **Add a README file**
5. اضغط **Create repository**

### الخطوة 3 — رفع ملف الداشبورد
1. في الـ Repo الجديدة، اضغط **Add file** ← **Upload files**
2. ارفع ملف `dashboard.html`
3. **المهم:** أعِد تسميته إلى `index.html` عند الرفع
4. اضغط **Commit changes**

### الخطوة 4 — تفعيل GitHub Pages
1. في الـ Repo ← **Settings** ← **Pages** (من القائمة اليسرى)
2. Under **Source**: اختر **Deploy from a branch**
3. **Branch**: `main` ← `/ (root)`
4. اضغط **Save**
5. انتظر دقيقتين

### الخطوة 5 — الرابط الخاص بك
```
https://YOUR_GITHUB_USERNAME.github.io/ahmed-farag-dashboard/
```

**مثال:** إذا اسم المستخدم `ahmed123`:
```
https://ahmed123.github.io/ahmed-farag-dashboard/
```

---

## ربط الرابط بالبوت

افتح `telegram_bot.py` واجد هذا السطر:

```python
DASHBOARD_URL = ''  # Set this to your GitHub Pages URL after deployment
```

غيّره إلى:
```python
DASHBOARD_URL = 'https://YOUR_USERNAME.github.io/ahmed-farag-dashboard/'
```

ثم أعِد تشغيل البوت:
```powershell
python telegram_bot.py
```

---

## ما يظهر في الداشبورد

| العنصر | التفاصيل |
|--------|----------|
| **العنوان** | AHMED FARAG بخط Cinzel الذهبي مع تأثير shimmer |
| **الترتيب** | Top 10 عملات مرتبة بـ 1H PRICE CHANGE% (الأكثر صعوداً أولاً) |
| **الأعمدة** | 15 عمود: #، SYMBOL، PRICE، 1H%، 4H%، 24H%، 4H VOL، VOL Z، TPS، 1H TPS%، 4H TPS%، ATS، 1H ATS%، 4H ATS%، FUND% |
| **Heatmap** | **كل الخانات الرقمية** — أخضر للموجب، أحمر للسالب، متدرج حسب الحجم |
| **التحديث** | كل 30 ثانية تلقائياً |
| **الميدالية** | 🥇🥈🥉 لأول 3 عملات |

---

## الزر في التيليجرام

بعد إضافة الرابط، سيظهر زر في أسفل كيبورد البوت:

```
[ 📊 Ahmed Farag Dashboard ]
```

عند الضغط عليه يفتح الداشبورد مباشرة داخل تيليجرام.

---

> **ملاحظة:** GitHub Pages مجاني تماماً ولا يحتاج بطاقة ائتمان.
> البيانات تُجلب مباشرة من Binance API في المتصفح، لا تحتاج سيرفر.
