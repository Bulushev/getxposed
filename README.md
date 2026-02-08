# Telegram Status Bot

Минимальный бот для анонимных оценок по категориям.

## Запуск

1. Создай виртуальное окружение и установи зависимости:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Создай `.env` и положи токен и (для облака) `DATABASE_URL`:

```bash
BOT_TOKEN=...
DATABASE_URL=postgresql://user:password@host:5432/dbname
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
- В App Platform рекомендуется Postgres, т.к. локальный файл `data.sqlite3` не сохраняется между деплоями.

Реферальная ссылка вида `https://t.me/<bot>?start=ref_username` открывает сразу форму оценки для этого пользователя.
