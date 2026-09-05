"""المترجم الفوري — خادم الترجمة والنطق (v2.1 — Telnyx primitives)"""
import asyncio
import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Form, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Telnyx primitives — طبقة موحدة
import sys
sys.path.insert(0, "/home/ubuntu/nuba-ai")
from telnyx_primitives import tts_sync as telnyx_tts, translate_sync as telnyx_translate, stt_sync as telnyx_stt

from starlette.middleware.base import BaseHTTPMiddleware

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """مهارة security-and-hardening: رؤوس الأمان الأساسية ضد XSS/Clickjacking/MIME"""
    async def dispatch(self, request, call_next):
        resp = await call_next(request)
        resp.headers['X-Content-Type-Options'] = 'nosniff'
        resp.headers['X-Frame-Options'] = 'DENY'
        resp.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        resp.headers['Cache-Control'] = 'no-cache, must-revalidate'
        # أندرويد قديم يفسر (self) بصرامة فيمنع الميكروفون عبر النفق — نسمح صراحة
        resp.headers['Permissions-Policy'] = 'microphone=*, camera=(), geolocation=()'
        return resp

app = FastAPI(title="Nuba AI — المترجم الفوري", version="3.0-live")

STATIC = Path(__file__).parent / "static"
CACHE = Path(tempfile.gettempdir()) / "tts_cache"
CACHE.mkdir(parents=True, exist_ok=True)

TELNYX_KEY = os.environ.get("TELNYX_STT_API_KEY", "")
STT_SCRIPT = "/home/ubuntu/nuba-ai/fast_stt.py"

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "live-translator"}


async def _gtx(text: str, source: str, target: str) -> dict:
    """الترجمة عبر نقطة Google المجانية (كشف تلقائي للمصدر)."""
    params = {"client": "gtx", "sl": source or "auto", "tl": target, "dt": "t", "q": text}
    async with httpx.AsyncClient(timeout=12) as c:
        r = await c.get(
            "https://translate.googleapis.com/translate_a/single",
            params=params,
            headers=UA,
        )
        r.raise_for_status()
        data = r.json()
    segs = data[0] or []
    translated = "".join(s[0] for s in segs if isinstance(s, list) and s and s[0])
    detected = data[2] if len(data) > 2 and data[2] else (source or "?")
    return {"text": translated, "detected": detected}


async def _lingva(text: str, target: str) -> dict:
    """خطة بديلة: خوادم Lingva مفتوحة."""
    import urllib.parse

    for host in ("https://lingva.ml", "https://lingva.garudalinux.org"):
        try:
            url = f"{host}/api/v1/auto/{target}/{urllib.parse.quote(text, safe='')}"
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(url, headers=UA)
                if r.status_code == 200:
                    j = r.json()
                    if j.get("translation"):
                        return {"text": j["translation"], "detected": j.get("info", {}).get("detectedSource", "?")}
        except Exception:
            continue
    raise HTTPException(502, "كل خدمات الترجمة فشلت")


@app.get("/api/translate")
async def translate(
    text: str = Query(..., min_length=1, max_length=2000),
    target: str = Query(..., min_length=2, max_length=8),
    source: str = Query("auto", max_length=8),
):
    text = text.strip()
    if not text:
        raise HTTPException(400, "نص فارغ")
    try:
        return await _gtx(text, source, target)
    except HTTPException:
        raise
    except Exception:
        try:
            return await _lingva(text, target)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(502, "تعذر الاتصال بخدمة الترجمة — تحقق من الإنترنت")


@app.get("/api/tts")
def tts(text: str = Query(..., min_length=1, max_length=1500), lang: str = Query("ar")):
    """نطق عبر Telnyx (Bayan عربي حقيقي / Kokoro عالمي) مع كاش."""
    text = text.strip()
    if not text:
        raise HTTPException(400, "نص فارغ")
    key = hashlib.sha1(f"{lang}|{text}".encode()).hexdigest()[:24]
    wav_path = CACHE / f"{key}.wav"
    mp3_path = CACHE / f"{key}.mp3"
    if wav_path.exists():
        return FileResponse(wav_path, media_type="audio/wav")
    if mp3_path.exists():
        return FileResponse(mp3_path, media_type="audio/mpeg")
    try:
        p = telnyx_tts(text, lang)
        dest = wav_path if p.suffix == ".wav" else mp3_path
        p.replace(dest)
        media = "audio/wav" if dest.suffix == ".wav" else "audio/mpeg"
        return FileResponse(dest, media_type=media)
    except Exception as e:
        raise HTTPException(502, f"فشل توليد الصوت (Telnyx): {e}")


# ---- تحويل الكلام لنص عبر Telnyx (داخل عملية الخادم، بدون subprocess) ----
# توجيه طارق: العربية على أفضل محرك عربي متاح، وباقي اللغات على Deepgram nova-3.
# (Basira غير معروض عبر STT API — Mission Control فقط؛ Cohere هو الأسرع والأدق عربياً في قياساتنا:
#  Cohere 1.7s/dقة أعلى مقابل nova-3 3.2s على نفس المقطع؛ نضع nova-3 لكل اللغات غير العربية)

ENGINE_BY_TTS_LANG = {
    # توجيه طارق: العربية على أفضل محرك عربي، والباقي nova-3.
    # قياس حي بعد اختبار المستخدم: Cohere يفشل صامتاً على المقاطع الطويلة (>8s صوت)
    # بينما nova-3 يفهم الجمل العربية كاملة عبر عدة نتائج متتالية. القرار: كل اللغات nova-3
    # (نفس طلب المستخدم الأصلي!) — Arabic fallback إلى Cohere فقط للمقاطع القصيرة إن لزم.
    "ar": "nova3",
    "en": "nova3", "fr": "nova3", "es": "nova3", "de": "nova3",
    "tr": "nova3", "ru": "nova3", "hi": "nova3", "ur": "nova3",
    "fa": "nova3", "it": "nova3", "pt": "nova3", "ko": "nova3",
    "id": "nova3", "nl": "nova3", "he": "nova3", "el": "nova3",
    "sv": "nova3", "zh-CN": "nova3", "ja": "nova3",
}

# engine alias -> (WS engine name, extra params)
WS_ENGINE_CONFIG = {
    "nova3": ("Deepgram", "&language={lang}&model=deepgram/nova-3"),
    "cohere/ar-stt": ("Cohere", "&language={lang}"),
}


def _telnyx_stt_sync(audio_bytes: bytes, ext: str, language: str, engine_alias: str) -> str:
    """النسخة المتزامنة الكاملة — تُنفذ داخل خيط مستقل (Claude #6). حلقة الخيط
    تبقى حية عبر stt_bridge._loop (لا حلقة جديدة تُدمّر مهام keepalive عالقة
    — قياس 11:1x: كانت تقتل الطلب 502 أحياناً وتلوث النتيجة بمعطيات معلقة)."""
    import asyncio as _aio
    import stt_bridge as _sb
    _sb.ensure_bridge()
    if not _sb._wait_loop():
        raise RuntimeError("bridge loop dead")

    fut = _aio.run_coroutine_threadsafe(
        _telnyx_stt_inner(audio_bytes, ext, language, engine_alias), _sb._loop)
    return fut.result(timeout=300)


async def _telnyx_stt_inline(audio_bytes: bytes, ext: str, language: str, engine_alias: str) -> str:
    """STT عبر خيط مستقل — Claude #6: الاستقبال المباشر داخل event loop يعلق
    (نفس علة +8.3s). منطق الجمل المتتالية والمهل الديناميكية كما هو داخل _inner."""
    return await asyncio.to_thread(_telnyx_stt_sync, audio_bytes, ext, language, engine_alias)


async def _telnyx_stt_inner(audio_bytes: bytes, ext: str, language: str, engine_alias: str) -> str:
    """المنطق الأصلي (يُستدعى من خيط مستقل بحلقة خاصة)."""
    import websockets as _ws

    # 1) تحويل الصوت إلى mp3 16k mono (الصيغة الموثوقة) — فقط إن لم يكن mp3 جاهزاً
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as f:
        f.write(audio_bytes)
        in_path = Path(f.name)
    try:
        if ext in ("mp3",):
            mp3_path = in_path
        else:
            mp3_path = in_path.with_suffix(".mp3")
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-i", str(in_path), "-vn", "-ar", "16000", "-ac", "1",
                "-c:a", "libmp3lame", "-q:a", "5", str(mp3_path),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.wait(), timeout=60)

        engine = WS_ENGINE_CONFIG.get(engine_alias, WS_ENGINE_CONFIG["nova3"])[0]
        params = WS_ENGINE_CONFIG.get(engine_alias, WS_ENGINE_CONFIG["nova3"])[1].format(lang=language)

        ws_url = (
            f"wss://api.telnyx.com/v2/speech-to-text/transcription"
            f"?transcription_engine={engine}&input_format=mp3{params}"
        )
        print(f"[stt#6] ws_url: {ws_url}", flush=True)
        headers = {"Authorization": f"Bearer {TELNYX_KEY}"}
        pieces: list[str] = []

        # اتصال يدوي (بلا async with) حتى نقطع فوراً بعد النتيجة —
        # إغلاق websockets المهذب يعلق ~2.7s لأن Telnyx لا يجيب على close handshake.
        ws = await _ws.connect(ws_url, additional_headers=headers, open_timeout=15)
        try:
            data = mp3_path.read_bytes()
            for i in range(0, len(data), 65536):
                await ws.send(data[i:i + 65536])
            await ws.send('{"type": "CloseStream"}')
            # النتيجة قد تأتي كعدة رسائل is_final متتالية للجمل الطويلة.
            # الاستراتيجية: بعد أول نتيجة ننتظر 1.2s بين الرسائل (الجمل المتتالية تتباعد عادة <1s)
            # وبعد انتهاء التدفق الكلي نقطع فوراً. قبل أول نتيجة: مهلة أولية 10s.
            got_any = False
            # حد أقصى إجمالي يغطي كامل طول الصوت (40s صوت → حتى ~25s معالجة + هامش)
            audio_secs_total = max(10.0, len(data) / 8000)
            hard_deadline = asyncio.get_event_loop().time() + audio_secs_total + 8
            while asyncio.get_event_loop().time() < hard_deadline:
                # مهلة ديناميكية مرتبطة بطول الصوت المرسل:
                # mp3 64kbps mono ≈ 8000 bytes/ثانية. لكل ثانية صوت: مهلة إضافية بين الرسائل.
                # أسوأ فجوة مقاسة بين نتائج nova-3: 4.5s — لذلك: 6s أساس + 0.25s لكل ثانية صوت
                # (مقطع 40s → مهلة ~16s | مقطع 3s → 6.75s)
                audio_secs = max(1.0, len(data) / 8000)
                gap_timeout = 6.0 + (audio_secs * 0.25)
                timeout = 10 if not got_any else gap_timeout
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except Exception:
                    break
                if isinstance(raw, bytes):
                    continue
                d = json.loads(raw)
                if "errors" in d:
                    raise RuntimeError(json.dumps(d["errors"])[:150])
                if d.get("is_final") and d.get("transcript"):
                    pieces.append(str(d["transcript"]))
                    got_any = True
                elif d.get("type") == "CloseStream":
                    break
        finally:
            # قطع فوري — abort بدل إغلاق مهذب
            try:
                await asyncio.wait_for(ws.close(), timeout=0.2)
            except Exception:
                try:
                    ws.transport.close()
                except Exception:
                    pass
        return " ".join(pieces).strip()
    finally:
        for p in (in_path, mp3_path if 'mp3_path' in dir() else None):
            if p and Path(p).exists():
                try:
                    Path(p).unlink()
                except OSError:
                    pass


_r_query = {}   # query params الحية (تُملأ في endpoint)

@app.post("/api/stt")
async def stt(request: Request, audio: UploadFile = File(...), lang: str = Form("ar")):
    """يستقبل مقطع صوتي ويعيد النص — lang من query أو form (قراءة يدوية)."""
    global _r_query
    _r_query = request.query_params
    """يستقبل مقطع صوتي ويعيد النص عبر Telnyx — داخل العملية (سريع)."""
    import time as _time
    _t0 = _time.time()
    if not TELNYX_KEY:
        raise HTTPException(500, "مفتاح Telnyx غير مضبوط على الخادم")
    # 🎯 الحل الجذري: Query() لم يلتقط أبداً (3 قياسات) — نقرأ يدوياً من request
    from fastapi import Request as _Req
    _r = _r_query.get("lang")
    print(f"[stt-ep] query_lang={_r!r} lang_form={lang!r}", flush=True)
    if _r:
        lang = _r
    engine = ENGINE_BY_TTS_LANG.get(lang, "nova3")
    ext = (Path(audio.filename or "a.webm").suffix or ".webm").lstrip(".").lower()
    content = await audio.read()
    if not content:
        raise HTTPException(400, "ملف صوتي فارغ")
    _t1 = _time.time()  # بعد قراءة الملف
    try:
        text = await asyncio.wait_for(
            _telnyx_stt_inline(content, ext, lang.split("-")[0], engine), timeout=300
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, "انتهت مهلة التحويل الصوتي")
    except Exception as e:
        raise HTTPException(502, f"فشل التعرف على الكلام: {str(e)[:150]}")
    _t2 = _time.time()
    print(f"[STT timing] read={_t1-_t0:.2f}s pipeline={_t2-_t1:.2f}s total={_t2-_t0:.2f}s", flush=True)
    if not text:
        raise HTTPException(422, "لم يُعثر على كلام واضح — تحدث بصوت أعلى")
    return {"text": text, "engine": engine}


# ---- وضع البث الحي (Live Mode) — مترجم المؤتمرات ----
from fastapi import WebSocket as _WS, WebSocketDisconnect as _WSD
from live_mode import live_endpoint

@app.websocket("/ws/live")
async def ws_live(ws: _WS, lang: str = "en", tts_lang: str = "ar", tts: str = "0"):
    await live_endpoint(ws, lang, tts_lang, tts)


# ---- وضع القاعة متعددة اللغات (Multi-Lang) — نقاش بلغات مختلطة ----
from multi_lang import multi_lang_endpoint

@app.websocket("/ws/duo")
async def ws_duo(ws: _WS, lang_a: str = "en", lang_b: str = "ar", tts: str = "1"):
    from multi_lang import duo_lang_worker
    await ws.accept()
    await duo_lang_worker(ws, lang_a, lang_b, tts == "1")


@app.websocket("/ws/multi")
async def ws_multi(ws: _WS, tts_lang: str = "ar"):
    await multi_lang_endpoint(ws, tts_lang)


# ---- عامل اللابتوب الدائم (Laptop Worker Queue) ----
from agent_tasks import router as agent_router
from fastapi.responses import PlainTextResponse

@app.get("/agent/agent_worker.py", response_class=PlainTextResponse)
async def serve_worker_py():
    return PlainTextResponse(open("/home/ubuntu/translator-app/agent_worker.py", encoding="utf-8").read())

@app.get("/agent/install_worker.ps1", response_class=PlainTextResponse)
async def serve_install_ps1():
    return PlainTextResponse(open("/home/ubuntu/translator-app/install_worker.ps1", encoding="utf-8").read(), media_type="text/plain; charset=utf-8")

from agent_tasks import router as agent_router
app.include_router(agent_router)

# ---- جسر المكالمات والواتساب (Call & WhatsApp Translation Bridge) ----
from voice_bridge import router as voice_router
app.add_middleware(SecurityHeadersMiddleware)

# ---- مهارة observability: مقاييس حية ----
import metrics as _metrics

@app.get('/api/metrics')
async def api_metrics():
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(_metrics.snapshot())
app.include_router(voice_router)


# ---- واجهة 3D الاحترافية (Linear-style + Three.js globe) ----
app.mount("/app3d", StaticFiles(directory="/home/ubuntu/translator-app/static3d", html=True), name="app3d")


app.mount("/", StaticFiles(directory=str(STATIC), html=True), name="static")
