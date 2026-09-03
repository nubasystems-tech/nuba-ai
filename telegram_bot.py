"""بوت الترجمة الصوتي — تيليجرام
يرسل المستخدم ملاحظة صوتية بلغة → يرد البوت بالترجمة منطوقة بصوت + نص.
STT: Telnyx Cohere (عربي/إنجليزي) | الترجمة: Google gtx | TTS: gTTS
"""
import asyncio
import os
import subprocess
import tempfile
from pathlib import Path

import httpx

BOT_TOKEN = os.environ["TRANSLATOR_BOT_TOKEN"]
API = f"https://api.telegram.org/bot{BOT_TOKEN}"
STT_SCRIPT = str(Path.home() / ".hermes/scripts/telnyx_stt.py")

# لغة واجهة المستخدم -> (لغة STT، لغة نطق الرد)
MODES = {
    "ar": {"stt": "cohere/ar-stt", "speak": "en", "label": "العربية → إنجليزي"},
    "en": {"stt": "whisper", "speak": "ar", "label": "English → عربي"},
}


def tg(method: str, **data) -> dict:
    r = httpx.post(f"{API}/{method}", json=data, timeout=30)
    return r.json()


async def download_file(file_id: str) -> Path:
    info = tg("getFile", file_id=file_id)
    p = Path(info["result"]["file_path"])
    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{p}"
    tmp = Path(tempfile.mkdtemp()) / p.name
    with httpx.stream("GET", url, timeout=60) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_bytes():
                f.write(chunk)
    return tmp


def transcribe(audio: Path, engine: str) -> str:
    out = audio.with_suffix(".txt")
    env = {**os.environ}
    proc = subprocess.run(
        ["python3", STT_SCRIPT, str(audio), str(out), "ar", engine],
        capture_output=True, text=True, timeout=120, env=env,
    )
    return out.read_text(encoding="utf-8").strip() if out.exists() else ""


def translate(text: str, target: str) -> str:
    r = httpx.get(
        "https://translate.googleapis.com/translate_a/single",
        params={"client": "gtx", "sl": "auto", "tl": target, "dt": "t", "q": text},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
    )
    data = r.json()
    return "".join(s[0] for s in data[0] if isinstance(s, list) and s and s[0])


def tts_file(text: str, lang: str) -> Path:
    from gtts import gTTS
    tmp = Path(tempfile.mkdtemp()) / "reply.mp3"
    gTTS(text=text, lang=lang if lang != "zh-CN" else "zh").save(str(tmp))
    return tmp


def detect_lang(text: str) -> str:
    # كشف سريع: حروف عربية؟
    arabic = sum(1 for c in text if "\u0600" <= c <= "\u06FF")
    return "ar" if arabic > len(text) * 0.3 else "en"


async def handle_message(msg: dict):
    chat_id = msg["chat"]["id"]
    # الأوامر
    text = msg.get("text", "")
    if text.startswith("/start"):
        kb = {
            "inline_keyboard": [
                [{"text": "🎤 عربي → إنجليزي", "callback_data": "ar"}],
                [{"text": "🎤 English → عربي", "callback_data": "en"}],
            ]
        }
        tg("sendMessage", chat_id=chat_id,
           text="مرحباً! بوت الترجمة الصوتي 🌍\n\nاختر الاتجاه، ثم أرسل رسالة صوتية 🎤:",
           reply_markup=kb)
        return

    # أزرار الاتجاه
    if text.startswith("ar") or text.startswith("en"):
        MODE_STATE[chat_id] = text[:2]
        m = MODES[text[:2]]
        tg("sendMessage", chat_id=chat_id,
           text=f"✅ الاتجاه الآن: {m['label']}\nأرسل رسالة صوتية 🎤")
        return

    voice = msg.get("voice") or msg.get("audio")
    if not voice:
        tg("sendMessage", chat_id=chat_id, text="أرسل رسالة صوتية 🎤 أو /start لتغيير الاتجاه")
        return

    mode = MODES.get(chat_id, "ar")
    m = MODES[mode]
    status = tg("sendMessage", chat_id=chat_id, text="⏳ جارٍ الاستماع والترجمة…")

    try:
        # 1) تنزيل الصوت
        af = await download_file(voice["file_id"])
        # 2) تحويل لصيغة يدعمها STT إن لزم (ogg→mp3 عبر ffmpeg)
        if af.suffix in (".oga", ".ogg", ".opus") or voice.get("mime_type", "").startswith("audio/ogg"):
            mp3 = af.with_suffix(".mp3")
            subprocess.run(["ffmpeg", "-y", "-i", str(af), "-vn", "-b:a", "64k", str(mp3)],
                          capture_output=True, timeout=60)
            if mp3.exists() and mp3.stat().st_size > 0:
                af = mp3
        # 3) STT
        src_text = transcribe(af, m["stt"])
        if not src_text:
            tg("sendMessage", chat_id=chat_id, text="⚠️ لم أستطع سماع الكلام بوضوح — جرّب مرة أخرى بصوت أوضح")
            return
        # 4) ترجمة
        detected = detect_lang(src_text)
        target = m["speak"]
        translated = translate(src_text, target)
        # 5) رد نصي + صوتي
        arrow = "→"
        tg("sendMessage", chat_id=chat_id,
           text=f"🗣️ {src_text}\n{arrow} {translated}")
        snd = tts_file(translated, target)
        with open(snd, "rb") as f:
            httpx.post(f"{API}/sendVoice", data={"chat_id": chat_id}, files={"voice": f}, timeout=120)
    except Exception as e:
        tg("sendMessage", chat_id=chat_id, text=f"⚠️ خطأ: {str(e)[:200]}")


MODE_STATE = {}


async def main():
    offset = 0
    print("بوت الترجمة يعمل الآن...")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                r = await client.post(f"{API}/getUpdates", json={"timeout": 30, "offset": offset}, timeout=40)
                for upd in r.json().get("result", []):
                    offset = upd["update_id"] + 1
                    if "message" in upd:
                        await handle_message(upd["message"])
                    elif "callback_query" in upd:
                        cb = upd["callback_query"]
                        chat_id = cb["message"]["chat"]["id"]
                        MODE_STATE[chat_id] = cb["data"]
                        m = MODES[cb["data"]]
                        tg("answerCallbackQuery", callback_query_id=cb["id"])
                        tg("sendMessage", chat_id=chat_id,
                           text=f"✅ الاتجاه الآن: {m['label']}\nأرسل رسالة صوتية 🎤")
            except Exception as e:
                print("خطأ:", e)
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
