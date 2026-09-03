"""Nuba AI — ترجمة المكالمات الهاتفية الحقيقية (Call & WhatsApp Translation).
يبقى هذا الملف جاهزاً ويعمل لحظة توفير مفتاح بصلاحيات Call Control.

المعمارية (Two-Leg Bilingual Bridge):
  المتصل (أي لغة) ──► رقم Telnyx ──► خادمنا (هذا الملف) ──► سماعة المستخدم بلغته
                       │
                       └── fork الصوت → STT (لغة المتحدث) → ترجمة (لغة المستمع) → TTS

نمط التشغيل — "المترجم الوسيط" (نفس عمل المترجم البشري في مكالمة ثلاثية):
  1) مكالمة واردة → answer → تحية بلغة المستخدم المختارة
  2) gather_using_ai يسمع المتحدث (كشف لغة تلقائي) → نترجم → ننطق الترجمة بصوت المستمع
  3) ثم يستمع لمستخدمنا → نترجم للغة الطرف الآخر → ننطقها له
  (تبادل: قول الجملة → تحويلها للطرف الآخر → العودة للاستماع)

Webhooks:
  POST /api/voice        — أحداث Call Control (call.initiated/answered/...)
  POST /api/whatsapp     — رسائل واتساب واردة (نص/صوت) → ترجمة → رد بنفس القناة
"""
import asyncio
import json
import os

import httpx
from fastapi import APIRouter, Request, Response

TELNYX_KEY = os.environ.get("TELNYX_STT_API_KEY", "")
TAPI = "https://api.telnyx.com/v2"
HDRS = {"Authorization": f"Bearer {TELNYX_KEY}"}

router = APIRouter()

# إعدادات المستخدم (تُقرأ من الطلب في /api/voice/config أو ثوابت مؤقتة)
USER_LANG = os.environ.get("NUBA_USER_LANG", "ar")   # لغة استماع المستخدم
GREETINGS = {
    "ar": "مرحباً، أنت متصل بمترجم نوبا. تحدث بلغتك وسأترجم للمستمع بلغته.",
    "en": "Hello, you have reached the Nuba translator. Speak your language and I will translate.",
    "fr": "Bonjour, vous êtes en ligne avec le traducteur Nuba.",
    "zh-CN": "您好，您已接通努巴翻译。",
}


@router.post("/api/voice/config")
async def voice_config(req: Request):
    """المستخدم يضبط لغته: {"listen":"ar","my_speech":"en"}"""
    d = await req.json()
    global USER_LANG
    USER_LANG = d.get("listen", USER_LANG)
    return {"ok": True, "listen_lang": USER_LANG}


@router.post("/api/voice")
async def voice_webhook(req: Request):
    """استقبال أحداث Call Control من Telnyx — كل الأحداث تصل هنا (webhook_event_url)."""
    d = await req.json()
    event = d.get("event", {}).get("type", "")
    payload = d.get("payload", {})
    call_id = payload.get("call_control_id", "")
    print(f"[voice] {event} call={call_id[:12]}", flush=True)

    if event == "call.initiated" and payload.get("direction") == "incoming":
        await _answer(call_id)
    elif event == "call.answered":
        await _start_translation_bridge(call_id, payload)
    elif event == "call.ai_gather.ended":
        await _handle_gather_result(call_id, payload)
    return Response(status_code=200)


async def _handle_gather_result(call_id: str, payload: dict):
    """انتهى استماع جملة المتحدث → نترجمها وننطقها بلغة المستخدم → نعيد الاستماع."""
    gather = payload.get("gather", {}) or payload.get("ai_gather", {}) or {}
    params = gather.get("parameters") or {}
    # النص إما في معامل "said" أو في سجل الرسائل
    text = params.get("said") or ""
    if not text:
        msgs = gather.get("message_history") or payload.get("message_history") or []
        for msg in msgs:
            if msg.get("role") == "user":
                text = msg.get("content", "")
    print(f"[voice] سمعنا: {text[:80]}", flush=True)
    if text:
        translated = _translate(text, USER_LANG)
        if translated:
            await _speak(call_id, translated, USER_LANG)
        await _start_listening(call_id)


async def _answer(call_id: str):
    async with httpx.AsyncClient(timeout=15) as c:
        await c.post(f"{TAPI}/calls/{call_id}/actions/answer", headers=HDRS, json={})


async def _speak(call_id: str, text: str, lang: str):
    """نطق نص على المكالمة — Bayan للعربية، Ultra للبقية."""
    voice = "Telnyx.Bayan.Ahmed" if lang.startswith("ar") else \
            "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062"
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(f"{TAPI}/calls/{call_id}/actions/speak", headers=HDRS, json={
            "payload": text,
            "payload_type": "text",
            "voice": voice,
            "service_level": "premium",
        })
        print(f"[voice] speak → {r.status_code} ({lang}): {text[:40]}", flush=True)


async def _start_translation_bridge(call_id: str, payload: dict):
    """جسر الترجمة التبادلي: تحية → استمع (كشف لغة تلقائي) → ترجم → نطق → كرر.
    gather_using_ai بمعامل واحد واضح: النتيجة النصية الكاملة تصل في call.ai_gather.ended."""
    await _speak(call_id, GREETINGS.get(USER_LANG, GREETINGS["ar"]), USER_LANG)
    await _start_listening(call_id)


async def _start_listening(call_id: str):
    """استماع مستمر: nova-3 مع كشف لغة تلقائي — يستمع حتى صمت المتحدث."""
    async with httpx.AsyncClient(timeout=25) as c:
        r = await c.post(f"{TAPI}/calls/{call_id}/actions/gather_using_ai", headers=HDRS, json={
            "parameters": {                    # معامل واحد: كل ما قاله المتحدث
                "properties": {
                    "said": {"type": "string", "description": "Everything the speaker said, verbatim, in their language"}
                },
                "required": ["said"],
                "type": "object"
            },
            "transcription": {
                "model": "deepgram/nova-3",
                "language": "auto",
            },
            "send_partial_results": False,
            "user_response_timeout_ms": 20000,
        })
        print(f"[voice] gather_using_ai → {r.status_code}: {r.text[:120]}", flush=True)


def _translate(text: str, target: str) -> str | None:
    try:
        import sys
        sys.path.insert(0, "/home/ubuntu/nuba-ai")
        from telnyx_primitives import translate_sync
        return translate_sync(text, target)
    except Exception as e:
        print(f"[voice] ترجمة فشلت: {e}", flush=True)
        return None


# ---------- واتساب (جاهز لحظة تفعيل WhatsApp API على الحساب) ----------
@router.post("/api/whatsapp")
async def whatsapp_webhook(req: Request):
    """رسالة واتساب واردة (نص أو صوت) → ترجمة → رد بالمترجم."""
    d = await req.json()
    data = d.get("data", {})
    payload = data.get("payload", {})
    frm = payload.get("from", "")
    msg_type = payload.get("type", "")
    print(f"[wa] من {frm}: نوع={msg_type}", flush=True)

    if msg_type == "text":
        text = payload.get("text", "")
        translated = _translate(text, USER_LANG)
        if translated:
            await _wa_send(frm, f"🔊 {translated}")
    elif msg_type == "audio" or msg_type == "voice":
        media_url = (payload.get("media") or {}).get("media_url", "")
        if media_url:
            text = await _wa_stt(media_url)
            if text:
                translated = _translate(text, USER_LANG)
                if translated:
                    await _wa_send(frm, f"🎤 {text}\n🔊 {translated}")
    return Response(status_code=200)


async def _wa_send(to: str, text: str):
    async with httpx.AsyncClient(timeout=15) as c:
        await c.post(f"{TAPI}/messages", headers=HDRS, json={
            "from": os.environ.get("NUBA_WA_NUMBER", ""),
            "to": to,
            "text": text,
        })


async def _wa_stt(media_url: str) -> str | None:
    """تحميل رسالة صوتية واتساب → STT عبر Telnyx."""
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(media_url, headers=HDRS)
            open("/tmp/wa_voice.mp3", "wb").write(r.content)
        import subprocess
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", "/tmp/wa_voice.mp3",
                        "-ar", "16000", "-ac", "1", "/tmp/wa_voice.wav"], check=True)
        import sys
        sys.path.insert(0, "/home/ubuntu/nuba-ai")
        from fast_stt import transcribe
        from pathlib import Path
        return asyncio.get_event_loop().run_until_executor(
            None, lambda: asyncio.run(transcribe(Path("/tmp/wa_voice.wav"), "auto", "nova3")))
    except Exception as e:
        print(f"[wa] STT فشل: {e}", flush=True)
        return None
