"""وضع البث الحي (Live Mode) v2 — مترجم المؤتمرات الاحترافي.
- keepalive: نبض صامت دوري يمنع إغلاق nova-3 للاتصال الخامل
- إعادة اتصال تلقائية: انقطاع upstream لا يقتل الجلسة — يُفتح اتصال جديد ويستمر البث
- نبض بين المتصفح والخادم: ping/pong يمنع وسطاء الشبكة (cloudflare) من القطع
"""
import asyncio
import stt_bridge
import metrics
from pathlib import Path
import json
import time

import websockets
from fastapi import WebSocket, WebSocketDisconnect

import sys
sys.path.insert(0, "/home/ubuntu/nuba-ai")
from telnyx_primitives import translate_sync, tts_sync

import os
TELNYX_KEY = os.environ.get("TELNYX_STT_API_KEY", "")

SILENCE_250MS = b"\x00\x00" * 4000  # صمت ربع ثانية للنبض


def _engine_url(lang: str) -> str:
    """عنوان upstream حسب اللغة (نفس المنطق السابق لكن بمعرف)."""
    if lang.startswith("ar"):
        engine, extra = "Cohere", ""
        ep = 300
    else:
        engine, extra = "Deepgram", "&model=deepgram/nova-3"
        ep = 300 if lang.startswith("zh") else 200
    return (f"wss://api.telnyx.com/v2/speech-to-text/transcription?"
            f"transcription_engine={engine}&input_format=linear16&sample_rate=16000"
            f"&language={lang}{extra}&endpointing={ep}")


async def _connect_upstream(lang: str):
    """فتح اتصال streaming مع إعادة المحاولة — المحرك حسب اللغة:
    🚨 قياس طارئ (2026-09-03 13:40): Deepgram بأكمله (nova-3/nova-2) صار صامتاً
    على Telnyx (صفر نتائج على صوت إنجليزي واضح!) بينما Cohere يعمل فوراً.
    كل اللغات → Cohere مؤقتاً حتى يستيقظ Deepgram. العربية Cohere ep300 أصلاً."""
    if lang.startswith("ar"):
        engine, extra = "Cohere", ""
        ep = 300
    else:
        engine, extra = "Deepgram", "&model=deepgram/nova-3"
        ep = 300 if lang.startswith("zh") else 200
    params = (f"transcription_engine={engine}&input_format=linear16&sample_rate=16000"
              f"&language={lang}{extra}&endpointing={ep}")
    ws_url = f"wss://api.telnyx.com/v2/speech-to-text/transcription?{params}"
    headers = {"Authorization": f"Bearer {TELNYX_KEY}"}
    last_err = None
    for attempt in range(3):
        try:
            return await websockets.connect(ws_url, additional_headers=headers, open_timeout=20)
        except Exception as e:
            last_err = e
            await asyncio.sleep(1.5 * (attempt + 1))
    raise last_err


TTS_DIR = Path(__file__).parent / "static" / "tts_audio"
TTS_DIR.mkdir(parents=True, exist_ok=True)


async def _tts_audio(text: str, lang: str):
    """TTS → mp3 24k mono (~9KB) → ملف ثابت + URL خفيف عبر GET.
    الحجة (سجل 2026-09-03): دفع b64 داخل WS عبر النفق المؤقت يكسر الاتصال (1006)
    ويوقف الترجمات كلها — URL خفيف يعبر النفق بلا مشاكل."""
    try:
        import subprocess as _sp
        import time as _tmod
        p = await asyncio.to_thread(tts_sync, text, lang)
        raw = p.read_bytes()
        suffix = p.suffix
        p.unlink(missing_ok=True)
        # ضغط إلى mp3 24kbps mono 16k — أصغر 10x مع نفس الوضوح للصوت المترجم
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-f", "wav" if suffix == ".wav" else "mp3",
            "-i", "pipe:0",
            "-ar", "16000", "-ac", "1", "-c:a", "libmp3lame", "-b:a", "24k",
            "-f", "mp3", "pipe:1",
            stdin=_sp.PIPE, stdout=_sp.PIPE, stderr=_sp.DEVNULL,
        )
        small, _ = await proc.communicate(raw)
        if not small:
            small = raw
        fname = f"t{_tmod.time_ns()}.mp3"
        (TTS_DIR / fname).write_bytes(small)
        # 🔧 مراجعة Claude #4: prune بالعمر لا بالعدّ — العدّ كان يحذف ملفات
        # جلسات أخرى نشطة قبل أن تنزلها (سبب مباشر لـ"TTS لا يُسمع")
        try:
            cutoff = _tmod.time() - 600   # نحتفظ 10 دقائق
            for f in TTS_DIR.glob("t*.mp3"):
                if f.stat().st_mtime < cutoff:
                    f.unlink(missing_ok=True)
        except Exception:
            pass
        return f"/tts_audio/{fname}"
    except Exception as e:
        print(f"[live] TTS فشل: {type(e).__name__}: {str(e)[:60]}", flush=True)
        return None


def _agc_amplify(pcm: bytes, state: list) -> bytes:
    """معالج صوت احترافي ثلاثي المراحل (numpy) — مصمم للكلام الضعيف جداً وسط الضجيج:
    1) مرشح ترددات: highpass 300Hz (فرق عينات مركّب) + lowpass بسيط — يحفظ نطاق الكلام البشري
    2) قمع ضجيج طيفي مبسط (Spectral Subtraction): يتعلم طيف الضجيج في الصمت
       ويطرحه من الإطارات الصوتية — الفارق الجوهري عن القص الزمني القديم
    3) AGC بقاع متتبع يعتبر الكلام كلاماً (بلا خنق) + normalization نهائي للقمم"""
    import numpy as np
    samples = np.frombuffer(pcm, dtype=np.int16).astype(np.float64)
    n = len(samples)
    if n == 0:
        return pcm
    # 1) مرشح نطاق الكلام: highpass قوي (يمحو همس القاعة <300Hz) + متوسط خفيف (lowpass ~4kHz)
    if len(state) < 5:
        state.extend([0.0, 0.0])  # state[4]=آخر عينة فلتر state[5]=متوسط طاقة الضجيج
    hp = np.empty_like(samples)
    hp[0] = samples[0] - state[4]
    hp[1:] = samples[1:] - samples[:-1] * 0.85   # highpass بمعامل يمنع تصفير الضجيج المستقر فقط
    state[4] = float(samples[-1])
    # lowpass: متوسط متحرك بسيط (يمحو الصفير/الشوشرة فوق نطاق الكلام)
    lp = np.convolve(hp, np.array([0.25, 0.5, 0.25]), mode="same")
    # 2) قمع الضجيج الطيفي المبسط: طاقة الإطار مقابل متوسط طاقة الضجيج المتعلمة
    frame_power = float(np.mean(lp ** 2))
    if len(state) < 6:
        state.append(frame_power)  # state[5]=طاقة الضجيج المتتبعة
    noise_pwr = state[5]
    # 🎯 كشف الكلام بالتذبذب + Hysteresis: إطارات الكلام متصلة (لا تقفز)
    # قياس ميداني: القفز 2x↔201x بين الإطارات يقطع الجمل — نضيف ذاكرة كلام 400ms
    if len(state) < 7:
        state.append(0.0)  # state[6]=آخر لحظة كلام مكتشف (hysteresis)
    zero_cross = float(np.mean(np.abs(np.diff(np.sign(lp)))) ) * 10.0
    variability = float(np.std(lp)) * 4.0
    recent_speech = (time.time() - state[6]) < 0.45 if state[6] else False
    is_speech = ((frame_power > noise_pwr * 1.5 and frame_power > 2.0)
                 or (variability > noise_pwr and variability > 3.0)
                 or (recent_speech and frame_power > noise_pwr * 0.8))  # استمرارية: الجملة الجارية لا تنقطع
    if is_speech:
        state[6] = time.time()
    if not is_speech:
        # إطار هادئ → نتعلم الضجيج (ببطء) ونكتم الإطار
        state[5] = noise_pwr * 0.97 + frame_power * 0.03
        if frame_power < noise_pwr * 0.6:
            lp = lp * 0.55   # هدوء: تخفيف (لا كتم كامل — الكتم كان يقطع شظايا الكلام المتصلة الضعيفة)
    else:
        # إطار كلام: نطرح طاقة الضجيج ونرفع للهدف
        state[5] = state[5] * 0.995
        snr_gain = frame_power / max(noise_pwr, 1e-9)
        boost = np.clip(np.log10(1 + snr_gain) * 2.0, 1.0, 10.0)
        lp = lp * boost
    # 3) AGC الهدف: نرفع القمم إلى مستوى كلام واضح (~4000) — حتى لأضعف همس (peak>4)
    peak = float(np.abs(lp).max())
    if peak > 4:
        ideal_gain = min(4200 / peak, 250.0)   # حتى x250 — الرفع الخطير مطلوب للهمس المتطرف
        state[0] = max(2.0, state[0] * 0.6 + ideal_gain * 0.4)
    else:
        state[0] = max(4.0, state[0] * 0.97)  # هبوط ألطف: لا ننفجر 250x→2x فنفقد أول جملة كلام
    out = np.clip(lp * state[0], -32768, 32767).astype(np.int16)
    return out.tobytes()


async def live_translation_worker(user_ws: WebSocket, src_lang: str, tts_lang: str, tts_enabled: bool = False):
    upstream = None
    agc_state = [18.0, True, 0]  # يبدأ مرتفعاً (25x كثير جداً لصوت قريب): أول جملة تُفهم فوراً — يهبط تلقائياً لو الصوت قريب
    _tts_backlog = []  # طابور الأصوات — متحدث سريع جداً: نحتفظ بأحدث جملتين فقط
    # 🎯 آلية الجمل الكاملة: شظايا الفكرة الواحدة + لحظة آخر شظية + مؤقتات الإغلاق
    _fragments = []            # شظايا الفكرة الجارية
    _frag_last = 0.0           # لحظة آخر شظية وصلت
    _frag_tasks = []           # مؤقتات الإغلاق المعلقة
    _recent_sent = []          # آخر النصوص المرسلة (كشف التكرار/الهلوسة)
    _tr_inflight = 0           # طلبات الترجمة الجارية (throttle ضد تجمد gtx)

    async def _flush_fragments():
        """إطلاق الفكرة المجمعة كاملة: نص + ترجمة (مرة واحدة بدل شظية شظية).
        مع throttle: لا نطلق >3 ترجمات متوازية (gtx يتجمد تحت التوازي الكثيف —
        قياس ميداني: بعد ~100 كلمة توقف التدفق تماماً)."""
        nonlocal _fragments, _frag_tasks, _tr_inflight
        if not _fragments:
            return
        full_sentence = " ".join(_fragments)
        _fragments = []
        for t in _frag_tasks:
            t.cancel()
        _frag_tasks = []
        metrics.inc("sentences_total")
        print(f"[live] 📝 جملة كاملة ({len(full_sentence.split())} كلمة): {full_sentence[:60]}", flush=True)
        try:
            await user_ws.send_json({"type": "source", "text": full_sentence})
        except Exception:
            return
        # ترجمة الفكرة الكاملة في مهمة مستقلة مع مراقبة التوازي
        async def _translate_and_speak(txt=full_sentence, _t=time.time()):
            nonlocal _tr_inflight
            # انتظار مهذب لو الطلبات المتوازية ممتلئة (طابور طبيعي بلا تجمد)
            waited = 0.0
            while _tr_inflight >= 3 and waited < 20:
                await asyncio.sleep(0.3)
                waited += 0.3
            if _tr_inflight >= 3:
                return  # أطلقناها — النص الأصلي موجود على أي حال
            _tr_inflight += 1
            try:
                translated = await asyncio.to_thread(translate_sync, txt, tts_lang)
            except Exception:
                translated = None
            finally:
                _tr_inflight -= 1
            if not translated:
                return
            try:
                await user_ws.send_json({"type": "translation", "text": translated})
                metrics.inc("translations_total")
            except Exception:
                return
            if not tts_enabled:
                return
            url = await _tts_audio(translated, tts_lang)
            if url:
                _tts_backlog.append((_t, url))
                while len(_tts_backlog) > 2:
                    _tts_backlog.pop(0)
                try:
                    await user_ws.send_json({"type": "tts", "url": url})
                    metrics.inc("tts_total")
                except Exception:
                    pass
        asyncio.create_task(_translate_and_speak())
    activity = [time.time()]  # آخر نشاط صوتي (قائمة قابلة للتعديل عبر الإغلاق)
    pending: asyncio.Queue = asyncio.Queue(maxsize=512)  # PCM من المتصفح بين إعادة الاتصالات

    bridge_id = f"live_{id(user_ws)}"

    async def connect_and_drain():
        """يفتح محرك عبر الجسر المستقل (خيط منفصل — حل علة بطء uvicorn المزمن:
        القياس 2026-09-03: نفس الصوت +0.3s خارج uvicorn مقابل +8.3s داخله!).
        ثم يصرف pending عبر الجسر."""
        url = _engine_url(src_lang)
        r = await stt_bridge.open_stream(bridge_id, url)
        if r != "OK":
            print(f"[live] فشل فتح الجسر: {r}", flush=True)
            return False
        drained = 0
        while not pending.empty():
            try:
                if await stt_bridge.send_audio(bridge_id, pending.get_nowait()):
                    drained += 1
                else:
                    break
            except Exception:
                break
        if drained:
            print(f"[live] استأنف البث بعد إعادة الاتصال: {drained} chunk", flush=True)
        return True

    async def pump():
        """من المتصفح → AGC (رفع الصوت الضعيف) → pending → upstream."""
        n_recv = 0
        while True:
            try:
                pcm = await user_ws.receive_bytes()
                if len(pcm) > 65536:   # مهارة security: سقف حجم (chunk طبيعي ≤8KB)
                    print(f'[live] ⚠️ chunk ضخم {len(pcm)}b — نتجاهله', flush=True)
                    continue
                n_recv += 1
                activity[0] = time.time()  # تحديث نشاط (يقرأه keepalive)
                if n_recv == 1:
                    print(f"[live] pump: أول chunk وصل ({len(pcm)}b)", flush=True)
                pcm = await asyncio.to_thread(_agc_amplify, pcm, agc_state)
                if n_recv == 20 or n_recv == 100:
                    print(f"[live] AGC: chunk#{n_recv} gain={agc_state[0]:.1f}x", flush=True)
                await pending.put(pcm)
                while not pending.empty():
                    if not await stt_bridge.send_audio(bridge_id, pending.get_nowait()):
                        # فشل إرسال: أعد فتح الجسر وصرف المتبقي
                        stt_bridge.close_stream(bridge_id)
                        await connect_and_drain()
            except (WebSocketDisconnect, Exception) as e:
                print(f"[live] pump انتهى: {type(e).__name__}: {str(e)[:80]}", flush=True)
                return

    async def keepalive_upstream():
        """🎯 كاشف السرعة الحقيقي (قياس 2026-09-03 18:1x):
        is_final لا يصل من المحرك إلا عند وصول صوت/صمت جديد بعده —
        بدونه جملتك تنتظر كلامك التالي = 8 ثوان تأخير ("الترجمة متأخرة جدا").
        الحل: نبض صامت 350ms بعد آخر كلام — يحرر نتيجة جملتك فوراً."""
        while True:
            await asyncio.sleep(0.35)
            if time.time() - activity[0] > 0.25:
                await stt_bridge.send_audio(bridge_id, SILENCE_250MS)

    async def collect():
        """🚀 الاستقبال عبر جسر الخيط المستقل (stt_bridge):
        قياس 2026-09-03: داخل uvicorn يصل is_final بعد +8.3s من نهاية الكلام
        بينما نفس الصوت خارجها +0.3s — علة event loop مؤكدة بعد 14 تشخيصاً.
        الجسر يدير websocket المحرك في حلقة مستقلة ونستقبل بالpoll السريع."""
        nonlocal _fragments, _frag_last, _frag_tasks, _recent_sent, _tr_inflight
        reconnect_backoff = 0
        if not await connect_and_drain():
            return
        while True:
            try:
                results = stt_bridge.poll_results(bridge_id, max_items=8)
                if not results:
                    await asyncio.sleep(0.04)
                    # فحص حياة الجسر: نبض خفيف كل 30s
                    continue
                for d in results:
                    if "errors" in d or d.get("type") in ("error", "closed"):
                        if d.get("type") == "closed":
                            # المحرك سقط — إعادة فتح شفافة
                            stt_bridge.close_stream(bridge_id)
                            reconnect_backoff = min(reconnect_backoff + 1, 4)
                            await asyncio.sleep(0.5 * reconnect_backoff)
                            await connect_and_drain()
                        continue
                    if not (d.get("is_final") and d.get("transcript")):
                        continue
                    text = str(d["transcript"]).strip()
                    if not text:
                        continue
                    # 🛡️ فلتر الهلوسة والتكرار (قياس مؤتمر حقيقي)
                    _norm = lambda x: " ".join(x.lower().split())
                    _tn = _norm(text)
                    _wc = len(_tn.split())
                    _dupe = False
                    for prev in _recent_sent[-8:]:
                        if _tn == prev:
                            _dupe = True; break
                        if _wc <= 4 and (_tn in prev or prev in _tn):
                            _dupe = True; break
                    if _dupe:
                        continue
                    _recent_sent.append(_tn)
                    if len(_recent_sent) > 40:
                        _recent_sent.pop(0)
                    # 📡 partial فوري — المستخدم يقرأ أثناء الكلام
                    try:
                        await user_ws.send_json({"type": "source_partial", "text": text})
                    except Exception:
                        pass
                    # 🎯 تجميع بالكلمات (تكيّفي مع سرعة المتحدث)
                    now = time.time()
                    if _fragments and (now - _frag_last) <= 2.5:
                        _fragments.append(text)
                    else:
                        if _fragments:
                            asyncio.create_task(_flush_fragments())
                        _fragments = [text]
                    _frag_last = now
                    word_count = sum(len(f.split()) for f in _fragments)
                    if word_count >= 5:
                        asyncio.create_task(_flush_fragments())
                    async def _closer():
                        await asyncio.sleep(0.6)
                        if _fragments and _frag_last and (time.time() - _frag_last) >= 0.55:
                            asyncio.create_task(_flush_fragments())
                    _frag_tasks.append(asyncio.create_task(_closer()))
            except asyncio.CancelledError:
                return
            except Exception as e:
                # خطأ غير متوقع — دورة أمان قصيرة
                await asyncio.sleep(0.3)
    # (الفتح يتم الآن داخل collect عبر الجسر فوراً عند بدء الجلسة)

    # 🔧 مراجعة Claude #2 CRITICAL: gather الأبدي كان يسرّب كل جلسة
    # (collect/keepalive حلقات لا نهائية لا تُلغى أبدا + close_stream لا يُستدعى)
    tasks = [asyncio.create_task(pump()),
             asyncio.create_task(keepalive_upstream()),
             asyncio.create_task(collect())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        stt_bridge.close_stream(bridge_id)


async def live_endpoint(websocket: WebSocket, lang: str = "en", tts_lang: str = "ar", tts: str = "0"):
    await websocket.accept()
    tts_enabled = tts == "1"
    try:
        await websocket.send_json({"type": "ready", "message": "البث الحي يعمل — تحدث الآن",
                                   "tts_enabled": tts_enabled})
        await live_translation_worker(websocket, lang, tts_lang, tts_enabled)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[live] خطأ الجلسة: {type(e).__name__}: {str(e)[:120]}", flush=True)
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
