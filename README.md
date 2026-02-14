# Telegram Contact Bot + Mini App

Бот для анонимных ответов о том, как лучше начинать контакт с человеком.

## Запуск

1. Создай виртуальное окружение и установи зависимости:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Создай `.env`:

```bash
BOT_TOKEN=...
DATABASE_URL=postgresql://user:password@host:5432/dbname
ADMIN_USERNAME=@bulushew
PORT=8080
MINI_APP_URL=https://your-domain/miniapp
BOT_USERNAME=getxposedbot
```

3. Запуск:

```bash
python main.py
```

## Как пользоваться

- Отправь в чат `@username` — появится форма ответа.
- Реферальная ссылка: `/ref @username`
- Статистика: `/stats @username`
- Админ-команды: `/admin_stats`, `/users`, `/normalize_case`
- Для платформ с health-check доступен эндпоинт `GET /health`.
- Mini App:
  - веб-страница: `GET /miniapp`
  - API профиля: `GET /api/miniapp/me`
  - API ответа: `POST /api/miniapp/feedback`
  - API инсайта: `GET /api/miniapp/insight?target=@username`
- В App Platform рекомендуется Postgres, т.к. локальный файл `data.sqlite3` не сохраняется между деплоями.

`MINI_APP_URL` должен быть публичным `https`-адресом (иначе Telegram не откроет WebApp).
