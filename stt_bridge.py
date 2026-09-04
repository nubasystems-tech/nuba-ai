"""stt_bridge.py — حل جذري لعلة الاستقبال داخل uvicorn:
الاتصال بمحركات Telnyx يُدار في خيط منفصل بحلقة أحداث مستقلة تماماً
(نفس بيئة الاختبارات المباشرة التي تعمل فوراً!) — الواجهة عبر thread-safe queues.
القياس المرجعي (2026-09-03 17:5x): المحرك يرد +0.0s خارج uvicorn لكن +8.3s داخله
— علة event loop/uvicorn مؤكدة بعد 14 تشخيصاً. هذا الجسر يتجاوزها كلياً."""
import asyncio
import json
import queue
import threading

import websockets

import os
TELNYX_KEY = os.environ.get("TELNYX_STT_API_KEY", "")

_loop = None          # حلقة الخيط المستقلة
_thread = None
_results = {}         # bridge_id → queue.Queue
_lock = threading.Lock()


import time as _time


def _bridge_loop():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()


def _wait_loop(timeout: float = 5.0):
    """انتظار جاهزية حلقة الخيط (سباق البداية)."""
    t0 = _time.time()
    while _loop is None and _time.time() - t0 < timeout:
        _time.sleep(0.02)
    return _loop is not None


def ensure_bridge():
    """تشغيل خيط الحلقة المستقلة مرة واحدة لكل عملية."""
    global _thread
    with _lock:
        if _thread is None or not _thread.is_alive():
            _thread = threading.Thread(target=_bridge_loop, daemon=True, name="stt-bridge")
            _thread.start()


async def open_stream(bridge_id: str, url: str):
    """فتح اتصال محرك — async (مراجعة Claude #1: لا حجب داخل event loop)."""
    ensure_bridge()
    if not _wait_loop():
        return "ERROR:bridge loop failed to start"
    fut = asyncio.run_coroutine_threadsafe(_open_and_pump(bridge_id, url), _loop)
    try:
        return await asyncio.wrap_future(fut)
    except Exception as e:
        return f"ERROR:{type(e).__name__}:{str(e)[:60]}"


async def _open_and_pump(bridge_id: str, url: str):
    """(داخل الخيط) فتح upstream وضخ النتائج إلى queue."""
    q = queue.Queue(maxsize=200)
    with _lock:
        _results[bridge_id] = q
    try:
        ws = await websockets.connect(
            url, additional_headers={"Authorization": f"Bearer {TELNYX_KEY}"},
            open_timeout=20)
    except Exception as e:
        q.put({"type": "error", "message": str(e)[:100]})
        return None

    async def pump():
        try:
            while True:
                raw = await ws.recv()
                if isinstance(raw, bytes):
                    continue
                try:
                    d = json.loads(raw)
                except Exception:
                    continue
                try:
                    q.put_nowait(d)
                except queue.Full:
                    pass
        except Exception as e:
            q.put({"type": "closed", "reason": type(e).__name__})
    asyncio.create_task(pump())
    # نعيد كائن تحكم بسيط عبر تخزين ws في dict بجانب الحلقة
    globals().setdefault("_sockets", {})
    _sockets = globals()["_sockets"]
    _sockets[bridge_id] = ws
    return "OK"


async def send_audio(bridge_id: str, pcm: bytes) -> bool:
    """إرسال صوت إلى upstream — async حقيقي (مراجعة Claude #1 CRITICAL:
    النسخة القديمة استدعت .result() المحجوب داخل event loop — يجمّد كل
    الجلسات المتزامنة عند أي بطء إرسال واحد!)."""
    ws = globals().get("_sockets", {}).get(bridge_id)
    if ws is None:
        return False
    fut = asyncio.run_coroutine_threadsafe(ws.send(pcm), _loop)
    try:
        await asyncio.wrap_future(fut)
        return True
    except Exception:
        return False


def poll_results(bridge_id: str, max_items: int = 10):
    """سحب النتائج المتاحة (non-blocking)."""
    with _lock:
        q = _results.get(bridge_id)
    if q is None:
        return []
    items = []
    for _ in range(max_items):
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            break
    return items


def close_stream(bridge_id: str):
    ws = globals().get("_sockets", {}).pop(bridge_id, None)
    if ws is not None:
        try:
            asyncio.run_coroutine_threadsafe(ws.close(), _loop).result(timeout=3)
        except Exception:
            pass
    with _lock:
        _results.pop(bridge_id, None)
