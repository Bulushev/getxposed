import re
from typing import Optional

import db

USERNAME_RE = re.compile(r"^@([A-Za-z0-9_]{3,32})$")


def normalize_username(raw: str) -> Optional[str]:
    raw = raw.strip()
    m = USERNAME_RE.match(raw)
    if not m:
        return None
    return f"@{m.group(1).lower()}"


def pick_recommendation(dimensions: dict[str, dict[str, int]]) -> tuple[str, str, str]:
    tone_counts = dimensions.get("tone", {})
    speed_counts = dimensions.get("speed", {})
    format_counts = dimensions.get("contact_format", {})

    tone_pick = "easy" if tone_counts.get("easy", 0) >= tone_counts.get("serious", 0) else "serious"
    speed_pick = "slow" if speed_counts.get("slow", 0) >= speed_counts.get("fast", 0) else "fast"
    format_pick = "text" if format_counts.get("text", 0) >= format_counts.get("live", 0) else "live"
    return tone_pick, speed_pick, format_pick


def normalize_feedback_value(value: str, allowed: set[str], default: str) -> str:
    return value if value in allowed else default


def _axis_pick(left: int, right: int, left_key: str, right_key: str) -> str:
    return left_key if left >= right else right_key


def _axis_is_uncertain(left: int, right: int) -> bool:
    total = left + right
    return total > 0 and (max(left, right) / total) < 0.6


def build_profile_payload(target: str) -> dict:
    target_user_id = db.get_user_id_by_username(target)
    total = db.get_total(target, target_user_id)
    ref_count = db.count_ref_visitors(target, target_user_id)
    dimensions = db.get_contact_dimensions(target, target_user_id)
    combined = total + ref_count
    viewed = int(combined * 1.4)
    silent = max(0, viewed - total)
    result = {
        "target": target,
        "viewed": viewed,
        "answers": total,
        "visitors": ref_count,
        "silent": silent,
        "enough": total >= 3,
        "recommendation": None,
        "caution_block": False,
        "uncertain_block": False,
        "result_rows": [],
        "extra_hint": "",
        "adaptive_questions": {
            "ask_tone_question": False,
            "ask_uncertainty_question": False,
        },
    }
    contact_left = (
        dimensions["tone"]["easy"]
        + dimensions["contact_format"]["text"]
        + dimensions["attention_reaction"]["likes"]
    )
    contact_right = (
        dimensions["tone"]["serious"]
        + dimensions["contact_format"]["live"]
        + dimensions["attention_reaction"]["careful"]
    )
    structure_left = dimensions["start_context"]["topic"]
    structure_right = dimensions["start_context"]["direct"]
    result["adaptive_questions"] = {
        "ask_tone_question": _axis_is_uncertain(contact_left, contact_right),
        "ask_uncertainty_question": _axis_is_uncertain(structure_left, structure_right),
    }
    if total < 3:
        return result

    tone_pick, speed_pick, format_pick = pick_recommendation(dimensions)
    result["recommendation"] = {
        "tone": tone_pick,
        "speed": speed_pick,
        "format": format_pick,
    }
    caution_counts = dimensions["caution"]
    result["caution_block"] = (caution_counts["true"] / total) >= 0.3 if total > 0 else False

    def is_uncertain(a: int, b: int) -> bool:
        s = a + b
        return s > 0 and max(a, b) / s < 0.6

    tone_counts = dimensions["tone"]
    speed_counts = dimensions["speed"]
    format_counts = dimensions["contact_format"]
    result["uncertain_block"] = (
        is_uncertain(tone_counts["easy"], tone_counts["serious"])
        or is_uncertain(speed_counts["fast"], speed_counts["slow"])
        or is_uncertain(format_counts["text"], format_counts["live"])
    )

    tempo_fast = dimensions["speed"]["fast"] + dimensions["frequency"]["often"]
    tempo_slow = dimensions["speed"]["slow"] + dimensions["frequency"]["rare"]
    initiative_active = dimensions["initiative"]["self"] + dimensions["caution"]["false"]
    initiative_wait = dimensions["initiative"]["wait"] + dimensions["caution"]["true"]
    contact_talk = (
        dimensions["tone"]["easy"]
        + dimensions["contact_format"]["text"]
        + dimensions["attention_reaction"]["likes"]
    )
    contact_reserved = (
        dimensions["tone"]["serious"]
        + dimensions["contact_format"]["live"]
        + dimensions["attention_reaction"]["careful"]
    )
    structure_flexible = dimensions["start_context"]["topic"]
    structure_specific = dimensions["start_context"]["direct"]

    tempo_pick = _axis_pick(tempo_fast, tempo_slow, "fast", "slow")
    initiative_pick = _axis_pick(initiative_active, initiative_wait, "active", "wait")
    contact_pick = _axis_pick(contact_talk, contact_reserved, "talk", "reserved")

    result["result_rows"] = [
        {
            "title": "Темп",
            "value": "Можно писать сразу и чаще" if tempo_pick == "fast" else "Лучше не спеша и без частых сообщений",
        },
        {
            "title": "Инициатива",
            "value": "Нормально, если инициативу проявляют" if initiative_pick == "active" else "Лучше аккуратно и без давления",
        },
        {
            "title": "Контакт",
            "value": "Легче начать с шутки и переписки" if contact_pick == "talk" else "Лучше спокойно, по делу и уважительно",
        },
    ]

    if structure_specific > structure_flexible:
        result["extra_hint"] = "Лучше конкретнее"
    elif _axis_is_uncertain(contact_talk, contact_reserved):
        result["extra_hint"] = "Человеку может понадобиться время на ответ"
    return result


def build_contact_insight_text(target: str) -> Optional[str]:
    target_user_id = db.get_user_id_by_username(target)
    total = db.get_total(target, target_user_id)
    if total < 3:
        return None

    dimensions = db.get_contact_dimensions(target, target_user_id)
    tone_counts = dimensions["tone"]
    speed_counts = dimensions["speed"]
    format_counts = dimensions["contact_format"]
    caution_counts = dimensions["caution"]

    tone_pick = "easy" if tone_counts["easy"] >= tone_counts["serious"] else "serious"
    speed_pick = "slow" if speed_counts["slow"] >= speed_counts["fast"] else "fast"
    format_pick = "text" if format_counts["text"] >= format_counts["live"] else "live"

    tone_text = "С юмора" if tone_pick == "easy" else "Спокойно, по делу"
    speed_text = "Не торопясь" if speed_pick == "slow" else "Сразу"
    format_text = "Через переписку" if format_pick == "text" else "В живом общении"

    lines = [
        "Как с этим человеком чаще всего",
        "начинают общение:",
        "",
        f"👉 {tone_text}",
        f"👉 {speed_text}",
        f"👉 {format_text}",
    ]

    def no_clear_majority(a: int, b: int) -> bool:
        s = a + b
        return s > 0 and max(a, b) / s < 0.6

    uncertain = (
        no_clear_majority(tone_counts["easy"], tone_counts["serious"])
        or no_clear_majority(speed_counts["fast"], speed_counts["slow"])
        or no_clear_majority(format_counts["text"], format_counts["live"])
    )
    if uncertain:
        lines += [
            "",
            "По этому пункту мнения разделились —",
            "лучше ориентироваться по ситуации.",
        ]

    caution_ratio = caution_counts["true"] / total if total > 0 else 0
    if caution_ratio >= 0.3:
        lines += [
            "",
            "⚠️ Иногда лучше не давить",
            "и дать время.",
        ]

    return "\n".join(lines)
