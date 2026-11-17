"""
سكريبت مساعد لرفع البوت على Railway أو Render
يحتوي على ملف Procfile المطلوب
"""
# هذا الملف مطلوب فقط إذا كنت تستخدم Webhook بدل Polling
# للاستضافة على Railway/Render

import os
from telegram_bot import main

if __name__ == "__main__":
    main()
