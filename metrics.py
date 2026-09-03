"""مهارة observability-and-instrumentation: مقاييس حية بلا مكتبات خارجية.
عدادات في الذاكرة + endpoint /api/metrics بصيغة Prometheus النصية."""
import time

_counters = {
    "ws_sessions_total": 0,       # كل اتصال بث بدأ
    "ws_active": 0,               # متصلون الآن
    "sentences_total": 0,         # جمل التُقطت
    "translations_total": 0,      # ترجمات أرسلت
    "tts_total": 0,               # أصوات أرسلت
    "reconnects_total": 0,       # إعادة اتصال upstream
    "errors_total": 0,            # أخطاء
}
_t0 = time.time()

def inc(name, n=1):
    _counters[name] = _counters.get(name, 0) + n

def dec(name, n=1):
    _counters[name] = max(0, _counters.get(name, 0) - n)

def snapshot() -> str:
    lines = []
    for k, v in _counters.items():
        lines.append(f"{k} {v}")
    lines.append(f"uptime_seconds {int(time.time() - _t0)}")
    return "\n".join(lines) + "\n"
