"""نقاط العامل على الخادم — طابور مهام اللابتوب:
- POST /agent/hello      تسجيل حضور العامل
- GET  /agent/next-task  سحب المهمة التالية (أقدم مهمة غير مسلمة)
- POST /agent/report     استلام النتيجة
- POST /agent/enqueue    (لأيرميس فقط) إضافة مهمة للطابور
الأمان: شبكة Tailscale الداخلية + توكن بسيط + صلاحيات القراءة للأول اثنين
والإضافة محصورة بالطلب من نفس الخادم (127.0.0.1) أو توكن صحيح."""
import asyncio
import time
import uuid

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter()

WORKER_TOKEN = "nuba-worker-2026"
TASKS = []          # [{task_id, kind, payload, created, status, result, worker}]
HELLOS = {}        # worker → آخر حضور


def _auth(request: Request, need_write: bool = False):
    tok = request.headers.get("X-Worker-Token", "")
    local = request.client.host.startswith("127.") or request.client.host.startswith("100.77.")
    if tok == WORKER_TOKEN:
        return
    if need_write:
        raise HTTPException(403, "توكن مطلوب للمهام الإدارية")
    if not local:
        raise HTTPException(403, "توكن غير صالح")


class TaskIn(BaseModel):
    kind: str
    payload: str
    note: str = ""


class ReportIn(BaseModel):
    task_id: str
    worker: str
    result: dict
    ts: float = 0


@router.post("/agent/hello")
async def hello(request: Request, body: dict = None):
    _auth(request)
    w = (body or {}).get("worker", "?")
    HELLOS[w] = time.time()
    return {"ok": True, "worker": w}


@router.get("/agent/next-task")
async def next_task(request: Request, worker: str = "?"):
    _auth(request)
    # استرجاع: عامل مات بعد التقاط مهمة → أعد الالتزامات الأقدم من 15 دقيقة للطابور
    # (مهلة المهمة 10 دقائق، فـ 15 دقيقة تعني أن الالتزام ميت فعلاً)
    now = time.time()
    for t in TASKS:
        if t["status"].startswith("claimed:") and now - t.get("claimed", 0) > 900:
            t["status"] = "queued"
            print(f"[agent] استرجاع مهمة {t['task_id']} — الالتزام مات", flush=True)
    for t in TASKS:
        if t["status"] == "queued":
            t["status"] = f"claimed:{worker}"
            t["claimed"] = time.time()
            return {"task_id": t["task_id"], "kind": t["kind"],
                    "payload": t["payload"], "note": t["note"]}
    return {"task_id": None}


@router.post("/agent/report")
async def report(request: Request, body: ReportIn):
    _auth(request)
    for t in TASKS:
        if t["task_id"] == body.task_id:
            t["status"] = "done"
            t["result"] = body.result
            t["worker"] = body.worker
            print(f"[agent] مهمة {body.task_id} اكتملت بواسطة {body.worker}: "
                  f"{'✓' if body.result.get('ok') else '✗'} {str(body.result.get('output',''))[:80]}", flush=True)
            return {"ok": True}
    raise HTTPException(404, "مهمة غير موجودة")


@router.post("/agent/enqueue")
async def enqueue(request: Request, body: TaskIn):
    _auth(request, need_write=True)
    tid = str(uuid.uuid4())[:8]
    TASKS.append({"task_id": tid, "kind": body.kind, "payload": body.payload,
                  "note": body.note, "created": time.time(), "status": "queued"})
    print(f"[agent] مهمة جديدة {tid}: {body.kind} — {body.note[:60]}", flush=True)
    return {"ok": True, "task_id": tid, "queued": sum(1 for t in TASKS if t["status"] == "queued")}


@router.get("/agent/status")
async def status(request: Request):
    _auth(request)
    now = time.time()
    return {
        "workers_online": {w: f"{int(now - t)}s ago" for w, t in HELLOS.items()
                           if now - t < 120},
        "queue": [{"id": t["task_id"], "kind": t["kind"], "status": t["status"],
                   "note": t["note"][:50],
                   "result": (t.get("result") or {}).get("output", "")[:150]}
                  for t in TASKS[-20:]],
    }
