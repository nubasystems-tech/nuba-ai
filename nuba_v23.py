"""Nuba AI v23 — TRANSLATOR CORE (نسخة نظيفة من الصفر)
مبادئ التصميم (من قياسات 22 نسخة + شكاوى ميدانية):
1. المحركات تهلوس على الصمت الخالص ('Thank you' من لا شيء) → أي is_final
   قادم خلال/بعد نبض صمت بلا طاقة صوت حقيقية = مهمل.
2. الهلوسات المكررة (MR.A ×14) → فلتر بصمة نصية قوي.
3. التأخير: نبض تحرير خفيف فقط عند وجود طاقة كلام فعلية قبل الصمت.
4. البساطة المطلقة: ملف واحد قابل للفهم كاملاً.

قناة الصوت: هاتف → WS (PCM 16k mono) → هذا الملف → Telnyx STT → ترجمة → هاتف
"""
import asyncio
import json
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import websockets

# ───────── الإعدادات (كلها في مكان واحد) ─────────
KEY = None                    # يُحقن من app.py (TELNYX_STT_API_KEY)
STT_URL = "wss://api.telnyx.com/v2/speech-to-text/transcription"
ENGINE_AR = ("Cohere", "&language=ar")                    # العربية: Cohere (مثبت)
ENGINE_EN = ("Deepgram", "&language=en&model=deepgram/nova-3")  # الإنجليزية: nova-3
TTS_URL = "https://api.telnyx.com/v2/text-to-speech/speech"
EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="nuba")

# عتبات المعالجة (كلها مُقاسة):
RMS_SPEECH = 50.0      # طاقة الكلام الفعلي (int16) — أقل من هذا = صمت/ضجيج
CONFIRM_SILENCE_S = 0.45  # كم ثانية صمت قبل إغلاق الجملة
PULSE_AFTER_S = 0.30      # نبض التحرير بعد توقف الكلام (لا قبله أبداً)
DUP_WINDOW_S = 12.0       # نافذة منع تكرار الجملة
MIN_WORDS = 2             # أقل من كلمتين = ليست جملة (تقتل MR.A وThank you المفردة؟
                          # لا — Thank you كلمتان: نستخدم قائمة سوداء للعبارات المهملة)

# 🔥 حفاظ على دفء المحرك: ضجيج هادئ طويل قد يجعل Telnyx/Cohere يغلق الجلسة
# منطقياً حتى لو استمر تدفق البايتات — أول جملة بعد ذلك تصل متأخرة لأنه يعيد
# الفتح. حل: كل KEEPALIVE_INTERVAL_S من الهدوء التام نُعيد بثّ آخر عيّنة صوت
# حقيقي (وليس أصفار) كنبضة "حياة" — ثم نكتم أي نتيجة تعود منها فوراً.
KEEPALIVE_INTERVAL_S = 20.0
KEEPALIVE_SUPPRESS_S = 2.5

# 👑 نافذة التتويج: عادية 0.35s. لكن Cohere أحياناً يرسل is_final لجزء من
# الجملة ثم is_final آخر أكمل بعد أكثر من 0.35s — فيولّد ذلك ترجمتين لجملة
# منطقية واحدة. عندما تبدو النتيجة "جزءاً" (قصيرة، بلا علامة ترقيم ختامية)
# نمنحها نافذة أطول قبل التتويج حتى تلحق النسخة الكاملة بنفس الدفعة.
CROWN_WAIT_S = 0.35
CROWN_WAIT_FRAGMENT_S = 0.90


def _looks_fragment(t: str) -> bool:
    """جملة تبدو غير مكتملة (قصيرة، بلا علامة ترقيم ختامية) → على الأغلب
    Cohere سيتبعها is_final أكمل خلال لحظات."""
    words = t.split()
    if not words or len(words) > 6:
        return False
    return t[-1] not in ".!?؟،۔"


# ───────── عبارات هلوسة شائعة (قياسات ميدانية: تظهر من الصمت الخالص) ─────────
HALLUCINATED = {
    "thank you", "thanks for watching", "thank you.", "thanks!", "thanks",
    "hello", "hello.", "hi", "hi.", "bye", "goodbye", "okay", "ok",
    "mr. a", "mra", "a", "yeah", "yes", "no", "mm", "hmm", "um",
    "thank you. thank you.", "bye!", "welcome", "you",
    # هلوسات صمت بحروف لاتينية (نقحرة عربية شائعة من محرك Cohere على الضجيج)
    "shnu anta", "shnu enta", "shino enta", "shino inta", "wesh",
}
# هلوسات الصمت بالعربية (قياسات ميدانية: "شنو انت" وأخواتها تظهر من ضجيج بحت)
HALLUCINATED_AR = {
    "شكرا", "شكرا لكم", "شكرا جزيلا", "شكراً", "مرحبا", "اهلا", "اهلا وسهلا",
    "مع السلامة", "الى اللقاء", "نعم", "لا", "حسنا", "طيب", "اوكي", "اوك",
    "امم", "هاه", "ايوه", "شنو انت", "شنو انتا", "شنو إنت", "وش فيه", "شو في",
}
# الترجمة الإنجليزية السريعة (gtx) — بلا مفاتيح، مع مزود احتياطي عند الفشل


def _translate_gtx(text: str, target: str) -> str:
    q = urllib.parse.quote(text)
    url = (f"https://translate.googleapis.com/translate_a/single"
           f"?client=gtx&sl=auto&tl={target}&dt=t&q={q}")
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            data = json.loads(r.read().decode())
            return "".join(seg[0] for seg in data[0] if seg[0])
    except Exception:
        return ""


def _translate_mymemory(text: str, target: str) -> str:
    """🔁 مزود احتياطي بلا مفتاح — يُستدعى فقط إذا فشل gtx مرتين.
    مهلته الخاصة 3s حتى لا يطيل زمن الاستجابة الكلي أكثر من اللازم."""
    src = "ar" if _lang_of(text) == "ar" else "en"
    q = urllib.parse.quote(text[:500])   # MyMemory يحد طول الاستعلام
    url = f"https://api.mymemory.translated.net/get?q={q}&langpair={src}|{target}"
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            data = json.loads(r.read().decode())
            t = (data.get("responseData") or {}).get("translatedText", "").strip()
            if t and "MYMEMORY WARNING" not in t.upper():
                return t
    except Exception:
        pass
    return ""


def translate_sync(text: str, target: str) -> str:
    """gtx أولاً (محاولتان سريعتان) ثم MyMemory كمزود ثانٍ عند فشل الأول
    تماماً — بلا نقطة فشل واحدة."""
    out = _translate_gtx(text, target)
    if out:
        return out
    time.sleep(0.2)
    out = _translate_gtx(text, target)
    if out:
        return out
    print(f"[v23] gtx فشل مرتين لـ '{text[:30]}' → تحويل لمزود احتياطي", flush=True)
    return _translate_mymemory(text, target)


def tts_sync(text: str, lang: str) -> str | None:
    """نطق عبر نطق المتصفح في العميل — الخادم لا يولّد صوتاً (أسرع وأخف)."""
    return None


def _norm_key(t: str) -> str:
    """بصمة نص: أحرف/أرقام فقط صغيرة — تقتل التكرار بأي علامات.
    isalnum()/lower() يعملان بشكل صحيح على العربية أيضاً (بلا حالة أحرف)."""
    return "".join(c for c in t.lower() if c.isalnum())[:60]


# تُبنى مرة واحدة عند التحميل (بدل إعادة بنائها في كل نداء) وتضم EN + AR معاً
_HALLUCINATED_NORM = {_norm_key(h) for h in (HALLUCINATED | HALLUCINATED_AR)}


def _is_hallucination(text: str) -> bool:
    """هل هذه الجملة هلوسة معروفة من الصمت؟ (إنجليزية أو عربية)."""
    k = _norm_key(text)
    if not k:
        return True
    if k in _HALLUCINATED_NORM:
        return True
    # MR.A ×14: تكرار نفس المقطع قصير ≥4 مرات = هلوسة حلزونية
    words = text.lower().split()
    if len(words) >= 4 and len(set(words)) <= 2:
        return True
    return False


def _lang_of(t: str) -> str:
    ar = sum(1 for c in t if "\u0600" <= c <= "\u06FF")
    letters = sum(1 for c in t if c.isalpha())
    if letters and ar / letters >= 0.35:
        return "ar"
    return "en"


class EngineSession:
    """اتصال STT واحد مع نبض تحرير ذكي: نبض صامت فقط بعد كلام حقيقي،
    والنتائج الهلوسة من الصمت تُقتل عند المصدر."""

    def __init__(self, name: str, engine: tuple):
        self.name = name                       # "ar" أو "en"
        self.engine, self.extra = engine
        self.ws = None
        self.last_speech_t = 0.0               # آخر لحظة كلام حقيقي (من العميل)
        self.pulse_sent = 0
        self.final_after_pulse = False         # هل آخر is_final جاء بعد نبض؟
        self.keepalive_sample: bytes | None = None  # آخر عينة صوت حقيقي (لإبقاء المحرك دافئاً)
        self.last_keepalive_t = 0.0
        self.suppress_until = 0.0              # حتى هذه اللحظة: اكتم أي نتيجة (رد نبضة حياة)

    async def connect(self):
        ep = 200
        url = (f"{STT_URL}?transcription_engine={self.engine}"
               f"&input_format=linear16&sample_rate=16000{self.extra}&endpointing={ep}")
        for attempt in range(3):
            try:
                self.ws = await websockets.connect(
                    url, additional_headers={"Authorization": f"Bearer {KEY}"},
                    open_timeout=15)
                return True
            except Exception as e:
                print(f"[v23:{self.name}] فتح {attempt}: {type(e).__name__}", flush=True)
                await asyncio.sleep(1 + attempt)
        return False

    async def send_audio(self, pcm: bytes, speech_now: bool):
        """الصوت يمر دائماً؛ النبض فقط عند الحاجة (بعد كلام حقيقي)."""
        if self.ws:
            try:
                await self.ws.send(pcm)
            except Exception:
                self.ws = None
                asyncio.get_running_loop().create_task(self._reconnect())
        if speech_now:
            self.last_speech_t = time.time()
            self.final_after_pulse = False     # كلام جديد: صفّر علم النبض
            self.keepalive_sample = pcm        # احتفظ بعينة صوت حقيقي لاستخدامها لاحقاً كنبضة حياة
        # نبض تحرير مزدوج الأمان:
        # (أ) بعد كلام مكتشف: نبضة واحدة بعد PULSE_AFTER_S (الطريق السريع)
        # (ب) بلا كلام مكتشف أصلاً: نبضة دورية كل 3s — لأن RMS قد يفشل الكشف
        #     (TTS المولد أخفض من العتبة) والتحرير يجب ألا يعلق أبداً.
        #     الفلاتر تتكفل بقتل أي هلوسة يولدها النبض الدوري.
        elif (self.last_speech_t and
              time.time() - self.last_speech_t >= PULSE_AFTER_S and
              not self.final_after_pulse):
            # نبضة تحرير واحدة بعد آخر كلام حقيقي — هذا كل شيء.
            # بلا نبض دوري: النبض على الضجيج الخافت المستمر أزعج Telnyx
            # (قياس السويت: المحرك أغلق بعد نبضات على بلا كلام)
            if self.ws:
                try:
                    await self.ws.send(b"\x00\x00" * 4000)
                    self.pulse_sent += 1
                except Exception:
                    pass
            self.final_after_pulse = True
        # 🔥 نبضة حياة على ضجيج هادئ طويل: صمت خالص لفترة طويلة قد يجعل
        # المحرك (Cohere خصوصاً) يغلق الجلسة منطقياً رغم استمرار تدفق البايتات،
        # فتصل أول جملة بعد ذلك متأخرة لأنه يعيد الفتح. نعيد بث آخر عينة صوت
        # حقيقي (وليس أصفار) كل KEEPALIVE_INTERVAL_S لإقناعه أن الجلسة حيّة،
        # ثم نكتم أي نتيجة تعود منها (suppress_until) حتى لا تتسرب للمستخدم.
        now = time.time()
        if (not speech_now and self.keepalive_sample and self.ws and
                self.last_speech_t and now - self.last_speech_t >= KEEPALIVE_INTERVAL_S and
                now - self.last_keepalive_t >= KEEPALIVE_INTERVAL_S):
            try:
                await self.ws.send(self.keepalive_sample)
                self.last_keepalive_t = now
                self.suppress_until = now + KEEPALIVE_SUPPRESS_S
                print(f"[v23:{self.name}] نبضة حياة (ضجيج هادئ طويل)", flush=True)
            except Exception:
                pass

    async def _reconnect(self):
        """إعادة فضم المحرك الميت تلقائياً (Telnyx يغلق على الضجيج الطويل)."""
        await asyncio.sleep(1.0)
        await self.connect()
        self.final_after_pulse = False

    async def results(self):
        """تيار النتائج: النص الفارغ والهلوسة القصيرة بعد نبضة = مقتولة فوراً."""
        if not self.ws:
            return
        try:
            while True:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=90)
                d = json.loads(raw)
                t = (d.get("transcript") or "").strip()
                if not t:
                    continue
                # 🔥 رد نبضة الحياة (صوت حقيقي أُعيد بثه فقط ليبقي المحرك دافئاً)
                # يُكتم كلياً — ليس محتوى حقيقياً من المتحدث الآن.
                if time.time() < self.suppress_until:
                    continue
                # 🛡️ القاعدة الذهبية: نتيجة قصيرة جاءت بعد نبض صمت = هلوسة صمت
                came_after_pulse = self.final_after_pulse and self.pulse_sent > 0
                if came_after_pulse and len(t.split()) <= 4 and _is_hallucination(t):
                    print(f"[v23:{self.name}] قتل هلوسة صمت: '{t[:30]}'", flush=True)
                    continue
                yield d
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            print(f"[v23:{self.name}] انقطع: {type(e).__name__}", flush=True)
            self.ws = None


async def translator_worker(ws, langs=("ar", "en"), tts_on=False):
    """الوضع الوحيد: عربي↔إنجليزي تبادلي. langs يتجاهل الترتيب — الكشف تلقائي."""
    await ws.accept()
    await ws.send_json({"type": "ready",
                        "message": "Nuba v23 جاهز — تحدث بحرية (عربي أو English)"})

    ar = EngineSession("ar", ENGINE_AR)
    en = EngineSession("en", ENGINE_EN)
    ok_ar, ok_en = await asyncio.gather(ar.connect(), en.connect())
    if not (ok_ar and ok_en):
        await ws.send_json({"type": "error", "message": "تعذر فتح محركات Telnyx"})
        await ws.close()
        return

    recent_keys = []            # [(key, t)] — منع تكرار الجمل
    finished = False
    pending = []                # نتائج is_final بانتظار التتويج
    crown_at = [0.0]            # موعد التتويج القادم (أو 0 = لا شيء)

    async def closer():
        """ينتظر تأكيد الصمت ثم يوحّد النتائج المجمعة في جملة واحدة."""
        while not finished:
            await asyncio.sleep(0.1)
            if crown_at[0] and time.time() >= crown_at[0] and pending:
                items, pending[:] = pending[:], []
                crown_at[0] = 0.0
                # اختيار النتيجة الصحيحة: نفس لغة المحرك (صدى اللغة الأخرى مقتول)
                best = None
                for d in items:
                    t = (d.get("transcript") or "").strip()
                    src = next(s for s in (ar, en) if s.name == d.get("_src", ""))
                    if _lang_of(t) == src.name:
                        if not best or len(t) > len(best[0]):
                            best = (t, src.name)
                if not best:
                    print(f"[v23] crown: لا صالح من {len(items)} نتيجة", flush=True)
                    continue
                text, src_lang = best
                print(f"[v23] crown: [{src_lang}] '{text[:35]}'", flush=True)
                # 🛡️ منع التكرار (MR.A ×14 مستحيل الآن)
                k = _norm_key(text)
                now = time.time()
                recent_keys[:] = [(x, t_) for (x, t_) in recent_keys if now - t_ < DUP_WINDOW_S]
                if any(x == k for x, _ in recent_keys):
                    print(f"[v23] تكرار محجوب: '{text[:30]}'", flush=True)
                    continue
                recent_keys.append((k, now))
                if _is_hallucination(text):
                    print(f"[v23] هلوسة محجوبة: '{text[:30]}'", flush=True)
                    continue

                target = "en" if src_lang == "ar" else "ar"
                try:
                    await ws.send_json({"type": "source", "text": text, "lang": src_lang})
                except Exception as e:
                    print(f"[v23] إرسال source فشل: {type(e).__name__}", flush=True)
                    continue
                loop = asyncio.get_running_loop()
                try:
                    # مهلة 10s: gtx (محاولتان×3s) + مزود احتياطي (3s) في أسوأ حال
                    translated = await asyncio.wait_for(
                        loop.run_in_executor(EXECUTOR, translate_sync, text, target),
                        timeout=10)
                except Exception as e:
                    print(f"[v23] ترجمة فشلت: {type(e).__name__}: {str(e)[:60]}", flush=True)
                    translated = ""
                if not translated:
                    print(f"[v23] ترجمة فارغة لـ [{text[:30]}] → {target}", flush=True)
                if translated:
                    try:
                        await ws.send_json({"type": "translation", "text": translated,
                                            "lang": target})
                        print(f"[v23] ✅ أُرسلت الترجمة: '{translated[:35]}' → {target}", flush=True)
                    except Exception as e:
                        print(f"[v23] إرسال ترجمة فشل: {type(e).__name__}", flush=True)

    async def collector(sess: EngineSession):
        while not finished:
            if not sess.ws:
                await asyncio.sleep(0.5)
                continue
            try:
                async for d in sess.results():
                    if finished:
                        break
                    t = (d.get("transcript") or "").strip()
                    if d.get("is_final"):
                        d["_src"] = sess.name
                        pending.append(d)
                        # 👑 نافذة تتويج متغيرة: نص "جزء" مشبوه (قصير، بلا ترقيم
                        # ختامي) يحصل على نافذة أطول ليلحق به is_final الكامل
                        # من Cohere ضمن نفس الدفعة بدل تتويجه منفرداً كترجمة ناقصة.
                        wait = CROWN_WAIT_FRAGMENT_S if _looks_fragment(t) else CROWN_WAIT_S
                        print(f"[v23:{sess.name}] is_final: '{t[:35]}' (نافذة {wait}s)", flush=True)
                        crown_at[0] = time.time() + wait
                    elif t and len(t.split()) >= 2:
                        # 📡 عرض فوري (partial): المستخدم يرى الجملة وهي تُقال
                        try:
                            await ws.send_json({"type": "source_partial",
                                                "text": t, "lang": sess.name})
                        except Exception:
                            pass
            except Exception:
                pass
            if not finished:
                await asyncio.sleep(0.8)
                if not sess.ws:
                    await sess.connect()

    async def pump():
        """من الهاتف: يمرر الصوت للمحركين + يحدد الكلام الحقيقي (للنبض)."""
        buf = np.empty(0, dtype=np.int16)
        while not finished:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=1.0)
                pcm = msg.get("bytes") if isinstance(msg, dict) else msg
                if pcm:
                    data = np.frombuffer(pcm, dtype=np.int16)
                    buf = np.concatenate((buf, data))[-16000:]   # آخر ثانية
                    rms = float(np.sqrt(np.mean(buf.astype(np.float32) ** 2))) if len(buf) else 0.0
                    speech_now = rms > RMS_SPEECH
                    raw = bytes(pcm)
                    await ar.send_audio(raw, speech_now)
                    await en.send_audio(raw, speech_now)
            except asyncio.TimeoutError:
                continue
            except Exception:
                break

    tasks = [asyncio.create_task(pump()), asyncio.create_task(closer())]
    tasks += [asyncio.create_task(collector(s)) for s in (ar, en)]
    done, pending_t = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finished = True
    for t in pending_t:
        t.cancel()
    for s in (ar, en):
        if s.ws:
            try:
                await s.ws.close()
            except Exception:
                pass
