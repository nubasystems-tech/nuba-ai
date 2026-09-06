"""
BRUTAL 1-HOUR ACADEMIC LECTURE SIMULATION
==========================================
Scenario: an Arabic-speaking student attends a 1-hour English scientific lecture
over the real-time duo translator (/ws/duo, lang_a=ar, lang_b=en).

- The professor speaks ENGLISH: fast (1.5x), quiet (10% volume), continuous hall
  noise mixed in (30%).
- The student asks 6 questions in ARABIC, normal volume/speed.
- Everything is streamed as ONE continuous ~55-60 minute WebSocket session in
  3200-byte PCM chunks every 0.2s, professor-rhythm (3-4 sentences, pause,
  occasional student question).

Audio pipeline: Telnyx Ultra TTS (mp3) -> ffmpeg (via the pip-installed static
binary from `imageio-ffmpeg`, since no system ffmpeg is present on this
machine) -> 16kHz mono s16le PCM. Degradation (volume/noise/speed) is then
applied in pure Python/numpy on the raw PCM samples, NOT via ffmpeg filters,
so the exact professor conditions requested are reproducible and inspectable.

Every 5 minutes the script prints a rolling measurement block; at the end it
prints a FINAL REPORT with PASS/FAIL per criterion, then LECTURE_SIM_DONE.
"""
import asyncio
import json
import os
import statistics
import struct
import subprocess
import tempfile
import time
from pathlib import Path

import numpy as np
import websockets

# ---------------------------------------------------------------------------
# Setup: load .env (repo-local), resolve ffmpeg (system if present, else the
# static binary bundled by imageio-ffmpeg — no admin install required)
# ---------------------------------------------------------------------------
HERE = Path(__file__).parent
ENV_FILE = HERE / ".env"
if ENV_FILE.exists():
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

KEY = os.environ.get("TELNYX_STT_API_KEY", "")
if not KEY:
    raise SystemExit("TELNYX_STT_API_KEY not set (checked process env and .env) — aborting.")

try:
    FFMPEG = subprocess.run(["where", "ffmpeg"], capture_output=True, text=True).stdout.strip().splitlines()
    FFMPEG = FFMPEG[0] if FFMPEG else None
except Exception:
    FFMPEG = None
if not FFMPEG:
    FFMPEG = "/usr/bin/ffmpeg"
print(f"[setup] ffmpeg: {FFMPEG}")

VOICE = "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062"
WS_URI = "ws://127.0.0.1:8000/ws/duo?lang_a=ar&lang_b=en&tts=0"
SR = 16000
LOG_PATH = HERE / "lecture_sim.log"


def _log(msg: str):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)


# ---------------------------------------------------------------------------
# TTS generation (Telnyx Ultra, same pattern as test_live.py) with retry —
# a transient DNS hiccup should not sink a 66-call generation batch.
# ---------------------------------------------------------------------------
def make_pcm(text: str, retries: int = 3) -> bytes:
    import httpx
    last_err = None
    for attempt in range(retries):
        try:
            r = httpx.post(
                "https://api.telnyx.com/v2/text-to-speech/speech",
                headers={"Authorization": f"Bearer {KEY}"},
                json={"text": text, "voice": VOICE, "audio_format": "mp3"},
                timeout=30,
            )
            r.raise_for_status()
            tmp = Path(tempfile.mkdtemp())
            mp3 = tmp / "s.mp3"
            mp3.write_bytes(r.content)
            raw = tmp / "s.raw"
            subprocess.run(
                [FFMPEG, "-y", "-i", str(mp3), "-ar", str(SR), "-ac", "1", "-f", "s16le", str(raw)],
                capture_output=True, timeout=60,
            )
            return raw.read_bytes()
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"TTS failed after {retries} attempts for {text[:40]!r}: {last_err}")


# ---------------------------------------------------------------------------
# Pure-python/numpy audio degradation (no ffmpeg filters — explicit control)
# ---------------------------------------------------------------------------
def degrade_professor(pcm: bytes, volume: float = 0.10, noise_mix: float = 0.30,
                       speed: float = 1.5, seed: int = 0) -> bytes:
    """Quiet + noisy + sped-up professor voice.

    volume: linear amplitude scale applied to the speech signal.
    noise_mix: the fraction of the FINAL mix's amplitude budget given to hall
        noise (white noise), i.e. final = (1-noise_mix)*speech + noise_mix*noise,
        with speech pre-scaled by `volume` first.
    speed: playback speed via simple linear resampling (index decimation) —
        cheap but adequate for a duration/rhythm-accurate STT stress test.
    """
    samples = np.frombuffer(pcm, dtype=np.int16).astype(np.float64)
    if len(samples) == 0:
        return pcm

    # 1) speed up 1.5x: resample by picking every 1/speed-th sample (linear
    #    interpolation to avoid pure frame-dropping artifacts)
    n_out = int(len(samples) / speed)
    src_idx = np.linspace(0, len(samples) - 1, n_out)
    idx_floor = np.floor(src_idx).astype(np.int64)
    idx_ceil = np.minimum(idx_floor + 1, len(samples) - 1)
    frac = src_idx - idx_floor
    sped = samples[idx_floor] * (1 - frac) + samples[idx_ceil] * frac

    # 2) lower volume
    quiet = sped * volume

    # 3) mix in continuous hall noise (white noise, deterministic per-clip seed
    #    so re-runs are reproducible, amplitude referenced to the ORIGINAL
    #    (pre-attenuation) signal level so the noise floor is realistically
    #    audible relative to a quiet speaker, not inaudibly tiny)
    rng = np.random.default_rng(seed)
    ref_amp = np.sqrt(np.mean(sped ** 2)) if len(sped) else 1000.0
    noise = rng.normal(0, max(ref_amp, 500.0) * 0.5, size=len(quiet))
    mixed = (1 - noise_mix) * quiet + noise_mix * noise

    mixed = np.clip(mixed, -32768, 32767).astype(np.int16)
    return mixed.tobytes()


def normal_pcm(pcm: bytes) -> bytes:
    """Student audio: unmodified, normal volume/speed."""
    return pcm


# ---------------------------------------------------------------------------
# Content: 60+ professor sentences (physics / AI / biology, lecture style,
# 8-25 words) and 6 Arabic student questions.
# ---------------------------------------------------------------------------
PROFESSOR_SENTENCES = [
    # --- Physics ---
    "Good morning everyone, today we begin our unit on quantum mechanics and wave-particle duality.",
    "Every particle in the universe can be described by a wave function that evolves according to the Schrodinger equation.",
    "When we measure the position of an electron, the wave function collapses to a single observed value.",
    "Heisenberg's uncertainty principle tells us we cannot simultaneously know both position and momentum with perfect precision.",
    "This is not a limitation of our instruments, it is a fundamental property of nature itself.",
    "Consider the double slit experiment, where a single photon somehow interferes with itself.",
    "The interference pattern only appears when we do not measure which slit the photon passed through.",
    "Entanglement allows two particles to share a correlated state even when separated by great distances.",
    "Einstein famously called this spooky action at a distance, though it does not violate causality.",
    "Now let us turn to general relativity and the curvature of spacetime around massive objects.",
    "Gravity, in this picture, is not a force but the geometry of spacetime itself.",
    "A massive star can collapse under its own gravity to form a black hole.",
    "Nothing, not even light, can escape once it crosses the event horizon of a black hole.",
    "Gravitational waves are ripples in spacetime produced by accelerating massive objects like merging black holes.",
    "LIGO detected these ripples in 2015, confirming a prediction Einstein made a century earlier.",
    "Thermodynamics gives us the arrow of time through the second law and increasing entropy.",
    "Entropy in an isolated system never decreases, which is why heat flows from hot to cold.",
    "Statistical mechanics connects the microscopic behavior of particles to macroscopic thermodynamic quantities like temperature.",
    "The standard model of particle physics describes all known fundamental particles and their interactions.",
    "Quarks combine in groups of three to form protons and neutrons inside atomic nuclei.",
    "The Higgs boson, discovered in 2012, explains why fundamental particles have mass at all.",
    # --- Artificial Intelligence ---
    "Let us shift now to artificial intelligence, starting with the basics of neural networks.",
    "A neural network is composed of layers of artificial neurons connected by weighted edges.",
    "During training, backpropagation adjusts these weights to minimize the error on training examples.",
    "Gradient descent iteratively moves the parameters in the direction that reduces the loss function.",
    "Deep learning simply means using many hidden layers to learn increasingly abstract representations.",
    "Convolutional neural networks are especially effective for image recognition tasks because they exploit spatial structure.",
    "Recurrent neural networks were designed to process sequential data such as text and speech.",
    "Transformers replaced recurrence with an attention mechanism that weighs the importance of every input token.",
    "Attention allows the model to focus on relevant parts of the input regardless of their distance.",
    "Large language models are trained on enormous text corpora to predict the next token in a sequence.",
    "Reinforcement learning trains an agent to maximize cumulative reward through trial and error.",
    "The exploration versus exploitation tradeoff is central to every reinforcement learning algorithm.",
    "Overfitting occurs when a model memorizes training data instead of learning generalizable patterns.",
    "Regularization techniques like dropout help prevent overfitting by adding noise during training.",
    "Transfer learning lets us reuse a model trained on one task to speed up learning on another.",
    "Generative adversarial networks pit two neural networks against each other in a competitive game.",
    "The generator tries to produce realistic samples while the discriminator tries to detect fakes.",
    "Diffusion models generate images by gradually denoising random noise into a coherent picture.",
    "Explainability remains a major challenge because deep networks behave as opaque black boxes.",
    "Bias in training data can lead to biased and unfair predictions from the resulting model.",
    # --- Biology ---
    "Finally, let us discuss molecular biology, beginning with the structure of deoxyribonucleic acid.",
    "DNA is a double helix made of two complementary strands held together by hydrogen bonds.",
    "The four nucleotide bases, adenine, thymine, guanine, and cytosine, encode all genetic information.",
    "Adenine always pairs with thymine, and guanine always pairs with cytosine, a rule called complementary base pairing.",
    "During transcription, an enzyme called RNA polymerase copies a gene into a strand of messenger RNA.",
    "The ribosome then reads the messenger RNA in groups of three nucleotides called codons.",
    "Each codon specifies a particular amino acid, which are linked together to form a protein.",
    "Proteins fold into complex three dimensional shapes that determine their biological function.",
    "Mutations are changes in the DNA sequence that can be silent, harmful, or occasionally beneficial.",
    "Natural selection acts on heritable variation, favoring traits that increase reproductive success.",
    "Over many generations, this process can lead to the evolution of entirely new species.",
    "Mitochondria are organelles that generate most of a cell's chemical energy through respiration.",
    "Photosynthesis in plants converts sunlight, water, and carbon dioxide into glucose and oxygen.",
    "The immune system distinguishes the body's own cells from foreign invaders like bacteria and viruses.",
    "Antibodies are proteins produced by immune cells that specifically bind to foreign antigens.",
    "Vaccines work by training the immune system to recognize a pathogen without causing the disease itself.",
    "CRISPR is a gene editing technology adapted from a natural bacterial immune defense mechanism.",
    "It allows scientists to precisely cut and modify specific sequences within a genome.",
    "Stem cells are unspecialized cells capable of developing into many different cell types.",
    "This concludes today's lecture, please review the slides and we will continue next week.",
]

STUDENT_QUESTIONS_AR = [
    "عفواً دكتور، هل يمكن أن توضح الفرق بين تراكب الحالات الكمومية والتشابك الكمي؟",
    "لم أفهم جيداً، لماذا لا نستطيع معرفة الموقع والزخم معاً بدقة كاملة؟",
    "بالنسبة للشبكات العصبية، ما الفرق العملي بين التعلم العميق والتعلم الآلي التقليدي؟",
    "هل يمكن أن تشرح مرة أخرى كيف يعمل الانتباه في نموذج المحول؟",
    "بخصوص الحمض النووي، كيف يمكن لطفرة واحدة أن تسبب مرضاً وراثياً خطيراً؟",
    "شكراً دكتور، سؤال أخير، هل ستتوفر تسجيلات هذه المحاضرة على الموقع؟",
]

assert len(PROFESSOR_SENTENCES) >= 60, f"need >=60 sentences, have {len(PROFESSOR_SENTENCES)}"
assert len(STUDENT_QUESTIONS_AR) == 6

TOTAL_EXPECTED = len(PROFESSOR_SENTENCES) + len(STUDENT_QUESTIONS_AR)

# ---------------------------------------------------------------------------
# Streaming plan: professor talks in bursts of 3-4 sentences, pauses 3-5s,
# a student question is interleaved roughly every ~10 professor sentences
# (6 questions across ~66 items), spread to fill ~55-60 minutes.
# ---------------------------------------------------------------------------
CHUNK = 3200          # bytes per send (~100ms of 16-bit mono 16kHz)
INTERVAL = 0.2         # seconds between chunk sends
INTER_SENTENCE_PAUSE = (8.0, 14.0)   # short breath between sentences in a burst
BURST_PAUSE = (25.0, 40.0)            # pause between professor bursts


def build_timeline():
    """Interleave professor sentences (in bursts of 3-4) with 6 student
    questions spread across the hour, returns a list of
    (kind, index_in_source, text) in playback order."""
    import random
    random.seed(42)
    timeline = []
    profs = list(enumerate(PROFESSOR_SENTENCES))
    q_positions = sorted(random.sample(range(8, len(profs) - 4), 6))
    qi = 0
    i = 0
    while i < len(profs):
        burst = random.choice([3, 4])
        for _ in range(burst):
            if i >= len(profs):
                break
            timeline.append(("prof", *profs[i]))
            if qi < len(q_positions) and i == q_positions[qi]:
                timeline.append(("student", qi, STUDENT_QUESTIONS_AR[qi]))
                qi += 1
            i += 1
    while qi < len(STUDENT_QUESTIONS_AR):
        timeline.append(("student", qi, STUDENT_QUESTIONS_AR[qi]))
        qi += 1
    return timeline


# ---------------------------------------------------------------------------
# Direction / language detection helpers (unicode-range based, per spec)
# ---------------------------------------------------------------------------
def is_arabic_text(t: str) -> bool:
    letters = [c for c in t if c.isalpha()]
    if not letters:
        return False
    ar = sum(1 for c in t if "؀" <= c <= "ۿ")
    return ar >= len(letters) * 0.4


def is_english_text(t: str) -> bool:
    letters = [c for c in t if c.isalpha()]
    if not letters:
        return False
    en = sum(1 for c in t if c.isascii() and c.isalpha())
    return en >= len(letters) * 0.6


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    t_script_start = time.time()
    _log("=" * 70)
    _log("LECTURE SIMULATION — generating audio (66 clips, this takes a while)")
    _log("=" * 70)

    timeline = build_timeline()
    clips = []  # list of (kind, src_index, text, pcm, duration_s)
    for n, (kind, idx, text) in enumerate(timeline):
        raw = await asyncio.to_thread(make_pcm, text)
        if kind == "prof":
            pcm = degrade_professor(raw, volume=0.10, noise_mix=0.30, speed=1.5, seed=idx)
        else:
            pcm = normal_pcm(raw)
        dur = len(pcm) / 2 / SR
        clips.append((kind, idx, text, pcm, dur))
        _log(f"  [{n+1}/{len(timeline)}] {kind:7s} ({dur:4.1f}s) {text[:55]}")

    total_audio_s = sum(c[4] for c in clips)
    _log(f"Audio generated: {len(clips)} clips, {total_audio_s/60:.1f} min of raw audio "
         f"(before pauses). Now streaming as ONE continuous session.")

    # -----------------------------------------------------------------
    # Measurement state
    # -----------------------------------------------------------------
    expected_events = []   # (t_sent_end, kind, idx, text) — for latency + coverage matching
    received = []          # (t_recv, msg_type, payload)
    stats = {
        "sentences_src_seen": 0,
        "translations_seen": 0,
        "hallucinations": 0,
        "duplicates": 0,
    }
    latencies = []          # seconds from end-of-speech to matching translation
    direction_checks = []   # True/False correctness per translation
    matched_expected = set()
    last_translation_by_key = {}   # normalized text -> last seen time (dup detection window 12s)
    speech_active_until = [0.0]    # time.time() until which "speech was recently sent" (hallucination check)

    t0 = time.time()

    def norm_key(t: str) -> str:
        return "".join(c.lower() for c in t if c.isalnum())[:40]

    def nearest_expected(now, kind_wanted):
        """Find nearest not-yet-matched expected event of matching source kind
        (prof-> should translate to ar, student-> should translate to en)."""
        best = None
        best_dt = None
        for i, (t_end, kind, idx, text) in enumerate(expected_events):
            if i in matched_expected:
                continue
            if kind != kind_wanted:
                continue
            dt = now - t_end
            if dt < -2:   # translation arriving before speech even ended (allow small negative slack)
                continue
            if best_dt is None or abs(dt) < abs(best_dt):
                best_dt = dt
                best = i
        return best, best_dt

    async def listener(ws):
        while True:
            # مهلة ديناميكية: أثناء بث الكلام النشط لا مهلة قاتلة — الصمت الطويل
            # بين دفعات الأستاذ قد يتجاوز 90s بلا نتائج جديدة (ضجيج يقتل الفلاتر)
            # بلا مهلة قاتلة: نعيش طوال البث — الفجوات الطويلة (25-40s)
            # بين دفعات الأستاذ طبيعية، والفلاتر قد تقتل كل هلوسات الضجيج
            # فلا يصل شيء للعميل دقائق — هذا صحيح وليس انهياراً.
            raw = await ws.recv()
            m = json.loads(raw)
            now = time.time()
            mtype = m.get("type")
            received.append((now, mtype, m))
            if mtype == "source":
                stats["sentences_src_seen"] += 1
                _log(f"  +{now-t0:7.1f}s 🎤 src[{m.get('speaker_lang')}]: {m.get('text','')[:60]}")
            elif mtype == "translation":
                text = m.get("text", "")
                speaker_lang = m.get("speaker_lang")
                target_lang = m.get("target_lang")
                stats["translations_seen"] += 1
                _log(f"  +{now-t0:7.1f}s 🌍 tr[{speaker_lang}->{target_lang}]: {text[:60]}")

                # --- hallucination check: no recent speech sent at all ---
                if now > speech_active_until[0] + 12 and not expected_events:
                    stats["hallucinations"] += 1

                # --- duplicate check: same normalized text within 12s ---
                key = norm_key(text)
                if key:
                    last_t = last_translation_by_key.get(key)
                    if last_t is not None and now - last_t < 12:
                        stats["duplicates"] += 1
                        stats["hallucinations"] += 1  # duplicate counts as a hallucination per spec
                    last_translation_by_key[key] = now

                # --- direction correctness ---
                # professor (en) speech -> should translate TO ar (student reads Arabic)
                # student (ar) speech -> should translate TO en (professor reads English)
                if target_lang == "ar":
                    correct = is_arabic_text(text)
                    wanted_kind = "prof"
                elif target_lang == "en":
                    correct = is_english_text(text)
                    wanted_kind = "student"
                else:
                    correct = False
                    wanted_kind = None
                direction_checks.append(correct)

                # --- latency: match to nearest not-yet-matched expected event ---
                if wanted_kind:
                    bi, dt = nearest_expected(now, wanted_kind)
                    if bi is not None and dt is not None and 0 <= dt <= 20:
                        matched_expected.add(bi)
                        latencies.append(dt)
            elif mtype == "error":
                _log(f"  ⚠️ server error: {m}")
            elif mtype == "ready":
                _log(f"  server ready: {m.get('message')}")

    async def reporter():
        """Print a rolling measurement block every 5 minutes."""
        next_mark = 300
        while True:
            await asyncio.sleep(10)
            elapsed = time.time() - t0
            if elapsed >= next_mark:
                _print_block(int(next_mark // 60))
                next_mark += 300

    def _print_block(minute_mark):
        n_expected_so_far = sum(1 for (t_end, *_r) in expected_events if t_end <= time.time() - t0)
        cov = (stats["translations_seen"] / n_expected_so_far * 100) if n_expected_so_far else 0.0
        dir_acc = (sum(direction_checks) / len(direction_checks) * 100) if direction_checks else 0.0
        med = statistics.median(latencies) if latencies else float("nan")
        _log("-" * 70)
        _log(f"[{minute_mark:3d} min mark] sentences_src={stats['sentences_src_seen']} "
             f"translations={stats['translations_seen']} expected_so_far~{n_expected_so_far} "
             f"coverage~{cov:.0f}% direction_acc~{dir_acc:.0f}% "
             f"median_latency={med:.1f}s hallucinations={stats['hallucinations']}")
        _log("-" * 70)

    async def sender(ws):
        for kind, idx, text, pcm, dur in clips:
            speech_active_until[0] = time.time() + dur + 15
            for i in range(0, len(pcm), CHUNK):
                await ws.send(pcm[i:i + CHUNK])
                await asyncio.sleep(INTERVAL)
            t_end = time.time()
            expected_events.append((t_end, kind, idx, text))
            import random as _r
            if kind == "prof":
                # short breath, unless this was the last of a burst — approximated
                # generically since burst boundaries are already encoded in build_timeline
                await asyncio.sleep(_r.uniform(*INTER_SENTENCE_PAUSE))
            else:
                await asyncio.sleep(_r.uniform(*BURST_PAUSE))
        # trailing silence so the final utterance's endpointing fires
        await asyncio.sleep(6)

    _log("Connecting to " + WS_URI)
    async with websockets.connect(WS_URI, open_timeout=20, max_size=None) as ws:
        ready = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
        _log(f"Server: {ready}")

        listen_task = asyncio.create_task(listener(ws))
        report_task = asyncio.create_task(reporter())
        send_task = asyncio.create_task(sender(ws))

        await send_task
        _log("All audio sent. Draining remaining translations for 20s...")
        await asyncio.sleep(20)

        report_task.cancel()
        listen_task.cancel()
        for t in (report_task, listen_task):
            try:
                await t
            except BaseException:
                pass

    # -----------------------------------------------------------------
    # FINAL REPORT
    # -----------------------------------------------------------------
    total_wall = time.time() - t_script_start
    n_translations = stats["translations_seen"]
    coverage_pct = (n_translations / TOTAL_EXPECTED * 100) if TOTAL_EXPECTED else 0.0
    direction_pct = (sum(direction_checks) / len(direction_checks) * 100) if direction_checks else 0.0
    dropped = TOTAL_EXPECTED - len(matched_expected)
    med_lat = statistics.median(latencies) if latencies else float("nan")
    p95_lat = (statistics.quantiles(latencies, n=100)[94] if len(latencies) >= 20
               else (max(latencies) if latencies else float("nan")))

    pass_coverage = coverage_pct >= 90.0
    pass_direction = direction_pct >= 95.0
    pass_latency = (p95_lat <= 3.0) if latencies else False
    pass_halluc = stats["hallucinations"] <= 2
    overall = "PASS" if all([pass_coverage, pass_direction, pass_latency, pass_halluc]) else "FAIL"

    _log("=" * 70)
    _log("FINAL REPORT — 1-HOUR LECTURE SIMULATION")
    _log("=" * 70)
    _log(f"Total wall time: {total_wall/60:.1f} min")
    _log(f"Expected sentences (60 professor + 6 student): {TOTAL_EXPECTED}")
    _log(f"Source-side sentences captured by server: {stats['sentences_src_seen']}")
    _log(f"Translations received: {n_translations}")
    _log(f"Translation coverage: {coverage_pct:.1f}% (need >=90%) -> {'PASS' if pass_coverage else 'FAIL'}")
    _log(f"Direction accuracy: {direction_pct:.1f}% over {len(direction_checks)} translations "
         f"(need >=95%) -> {'PASS' if pass_direction else 'FAIL'}")
    _log(f"Latency: median={med_lat:.2f}s p95={p95_lat:.2f}s (need p95<=3.0s) -> "
         f"{'PASS' if pass_latency else 'FAIL'}")
    _log(f"Hallucinations/duplicates: {stats['hallucinations']} (need <=2) -> "
         f"{'PASS' if pass_halluc else 'FAIL'}")
    _log(f"Dropped sentences (never matched to a translation): {dropped}/{TOTAL_EXPECTED}")
    _log(f"OVERALL VERDICT: {overall}")
    _log("=" * 70)
    _log("LECTURE_SIM_DONE")


if __name__ == "__main__":
    asyncio.run(main())
