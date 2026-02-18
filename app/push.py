from datetime import datetime
from typing import Callable, Optional

from aiogram import Bot

import db


class PushManager:
    def __init__(
        self,
        db_call: Callable,
        queue_coroutine: Callable,
        build_profile_payload: Callable[[str], dict],
        admin_username: str,
        push_timeout_seconds: float,
    ):
        self.db_call = db_call
        self.queue_coroutine = queue_coroutine
        self.build_profile_payload = build_profile_payload
        self.admin_username = admin_username
        self.push_timeout_seconds = push_timeout_seconds

    async def send_tracked_push(self, bot: Bot, target_id: int, text: str) -> bool:
        import asyncio

        try:
            await asyncio.wait_for(bot.send_message(target_id, text), timeout=self.push_timeout_seconds)
            return True
        except Exception as exc:
            target_username = (await self.db_call(db.get_username_by_user_id, target_id)) or f"id={target_id}"
            reason = f"{type(exc).__name__}: {exc}"
            reason_l = reason.lower()
            should_delete = (
                "bot was blocked by the user" in reason_l
                or "chat not found" in reason_l
                or "user is deactivated" in reason_l
                or "forbidden" in reason_l
            )
            if should_delete:
                await self.db_call(db.delete_user_by_user_id, target_id)

            admin_id = await self.db_call(db.get_user_id_by_username, f"@{self.admin_username}")
            if admin_id:
                try:
                    await asyncio.wait_for(
                        bot.send_message(
                            admin_id,
                            "Не удалось отправить push пользователю.\n"
                            f"Пользователь: {target_username}\n"
                            f"Причина: {reason}\n"
                            + ("Пользователь удалён из /users." if should_delete else "Пользователь НЕ удалён (временная ошибка)."),
                        ),
                        timeout=self.push_timeout_seconds,
                    )
                except Exception:
                    pass
            return False

    @staticmethod
    def is_quiet_hours() -> bool:
        hour = datetime.now().hour
        return hour >= 22 or hour < 9

    async def send_action_push(self, bot: Bot, target_id: int, event_type: str, text: str) -> bool:
        if self.is_quiet_hours():
            return False
        sent_today = await self.db_call(db.count_pushes_today, target_id)
        if sent_today >= 2:
            return False
        ok = await self.send_tracked_push(bot, target_id, text)
        if ok:
            await self.db_call(db.add_push_event, target_id, event_type)
        return ok

    async def process_feedback_submission(
        self,
        bot: Bot,
        target: str,
        voter_id: Optional[int],
        tone: str,
        speed: str,
        contact_format: str,
        initiative: str,
        start_context: str,
        attention_reaction: str,
        caution: str,
        frequency: str,
        comm_format: str,
        emotion_tone: str,
        feedback_style: str,
        uncertainty: str,
    ) -> tuple[Optional[str], str]:
        before_payload = await self.db_call(self.build_profile_payload, target)
        target_user_id = await self.db_call(db.get_user_id_by_username, target)
        result = await self.db_call(
            db.add_vote,
            target,
            "feedback",
            voter_id,
            target_user_id,
            tone,
            speed,
            contact_format,
            caution,
            initiative,
            start_context,
            attention_reaction,
            frequency,
            comm_format,
            emotion_tone,
            feedback_style,
            uncertainty,
        )
        if result is None:
            return None, "База недоступна, попробуй позже"
        if result == "duplicate_recent":
            return result, "Мнение можно менять не чаще 1 раза в сутки"

        target_id = target_user_id
        if target_id:
            after_payload = await self.db_call(self.build_profile_payload, target)
            answers_total = int(after_payload.get("answers") or 0)

            if result == "inserted" and answers_total > 0 and answers_total % 2 == 0:
                self.queue_coroutine(
                    self.send_action_push(
                        bot,
                        target_id,
                        "new_feedback",
                        "📝 про тебя ответили — появилось новое мнение о тебе",
                    )
                )

            before_rows = before_payload.get("result_rows") if isinstance(before_payload, dict) else []
            after_rows = after_payload.get("result_rows") if isinstance(after_payload, dict) else []
            before_hint = (before_payload or {}).get("extra_hint", "") if isinstance(before_payload, dict) else ""
            after_hint = (after_payload or {}).get("extra_hint", "") if isinstance(after_payload, dict) else ""
            if before_rows != after_rows or before_hint != after_hint:
                self.queue_coroutine(
                    self.send_action_push(
                        bot,
                        target_id,
                        "result_updated",
                        "🔄 подсказка о тебе обновилась — результат изменился",
                    )
                )

            referred_answers = await self.db_call(db.count_ref_answerers, target, target_id)
            if referred_answers > 0 and referred_answers % 2 == 0:
                self.queue_coroutine(
                    self.send_action_push(
                        bot,
                        target_id,
                        "ref_answer",
                        "🔗 по твоей ссылке отвечают — кто-то пришёл от тебя",
                    )
                )

        message = "Мнение обновлено." if result == "updated" else "Готово 👍\n\nТы помог понять,\nкак к этому человеку проще подойти."
        return result, message

    async def validate_feedback_target(self, bot: Bot, target: str) -> tuple[bool, Optional[str]]:
        import asyncio

        username = target.lstrip("@").lower()
        if username.endswith("bot"):
            return False, "Нельзя оставлять отзывы о ботах."
        try:
            chat = await asyncio.wait_for(bot.get_chat(target), timeout=3.0)
        except Exception:
            return True, None
        if chat.type in {"group", "supergroup"}:
            return False, "Нельзя оставлять отзывы о чатах."
        if chat.type == "channel":
            return False, "Нельзя оставлять отзывы о каналах."
        return True, None
