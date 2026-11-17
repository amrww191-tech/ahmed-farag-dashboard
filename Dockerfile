FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PORT=8080 \
    MPLBACKEND=Agg \
    BOT_TOKEN="8988728486:AAHw50P8y1SCdIupdn5aOTqyGxIJyzLH6_E" \
    ALLOWED_CHAT_ID="1686148743"

# تثبيت الحزم النظامية اللازمة لـ matplotlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    libfreetype6-dev \
    fontconfig \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

COPY requirements_bot.txt .
RUN pip install --no-cache-dir -r requirements_bot.txt

COPY telegram_bot.py .

EXPOSE 8080

CMD ["python", "telegram_bot.py"]
