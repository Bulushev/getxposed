# Telegram Status Bot

Минимальный бот для анонимных оценок по категориям.

## Запуск

1. Создай виртуальное окружение и установи зависимости:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Создай `.env` и положи токен:

```bash
BOT_TOKEN=...
```

3. Запуск:

```bash
python main.py
```

## Как пользоваться

- Отправь в чат `@username` — появится выбор оценки.
- Реферальная ссылка: `/ref @username`
- Статистика: `/stats @username`
- Уведомления приходят, если пользователь когда-либо запускал бота (`/start`).
- Для платформ с health-check доступен эндпоинт `GET /health`.

Реферальная ссылка вида `https://t.me/<bot>?start=ref_username` открывает сразу форму оценки для этого пользователя.
