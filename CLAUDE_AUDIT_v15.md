Read both files in full (live_mode.py 411 lines, multi_lang.py 914 lines) plus the browser client that feeds them (`static/live.html`), since the pulse gates depend on the client's send cadence.

---

## AUDIT — 10 FINDINGS

**1. CRITICAL | live_mode.py:302 + multi_lang.py:450,454 | Both release pulses are effectively dead code**
`static/live.html:161-171` sends a PCM chunk unconditionally on every `onaudioprocess` (4096/16000 = **every 256 ms**), speaking or not. Both gates measure idle since the last *received chunk* (`activity[0]` set at live_mode.py:279, `_last_audio[0]` at multi_lang.py:205), so `idle > 0.25` / `0.25 < idle < 3.0` is only true in the jitter tail — the 350 ms pulse and the duo pulse almost never fire, and `is_final` is left waiting on engine endpointing that never triggers (finding 2). This is the primary cause of multi-second release.
*Fix: gate on last detected **speech**, not last chunk — `_agc_amplify` already tracks it in `agc_state[6]`; expose it and test `time.time() - agc_state[6] > 0.25` (cadence unchanged).*

**2. CRITICAL | live_mode.py:146-166 | AGC amplifies inter-sentence silence, so endpointing never fires**
On a non-speech frame the code attenuates only to `lp * 0.55` and only when `frame_power < noise_pwr * 0.6` (line 152); the frame is then multiplied by `state[0]`, which decays at 0.97/frame from as high as 250x (line 165). Room noise is boosted to peak ≈4200 and the engine receives continuous audio — its 200/300 ms `endpointing` window is never satisfied, so `is_final` is released only by a pulse that (per 1) does not arrive. Applies to `multi_lang` too via the `_agc` alias at multi_lang.py:66.
*Fix: hard-zero the frame (`lp[:] = 0`) once `not is_speech` and `time.time() - state[6] > 0.2`, so real digital silence reaches the engine.*

**3. CRITICAL | live_mode.py:360-363 | Stale flush emits the wrong sentence and silently drops the previous one**
`asyncio.create_task(_flush_fragments())` is scheduled, then `_fragments = [text]` rebinds the nonlocal on the *next line*, before the task ever runs. When the task runs it reads the rebound list, joins the **new** text, and clears. The completed sentence that triggered the flush is never sent — its translation latency is infinite — and the new sentence is emitted alone. Fires on every speaker pause > 2.5 s.
*Fix: snapshot before scheduling — `old, _fragments = _fragments, [text]` then `create_task(_flush_fragments(old))`, with the coroutine taking the list as a parameter.*

**4. CRITICAL | live_mode.py:215,76 + multi_lang.py:212,578 | `wait_for` around `to_thread` does not free the worker thread; the shared executor starves the audio pump**
Cancelling `asyncio.wait_for` cancels the awaiting coroutine, never the blocking call already running in the default `ThreadPoolExecutor` (`min(32, cpu+4)` — 8 threads on a 4-core host). Every hung `translate_sync` (8 s cap) and `tts_sync` (10 s cap) permanently occupies a thread. The same executor serves the per-chunk `asyncio.to_thread(_agc, ...)` in `pump`, so once threads are exhausted **audio ingestion itself blocks** and every downstream stage stalls. `multi_lang.py:774,789,876` make it worse — no timeout at all there, so a single hung translate holds a thread for the session's life.
*Fix: give translate/TTS their own bounded `ThreadPoolExecutor` via `loop.run_in_executor` so AGC never contends with them, and add `timeout=8`/`timeout=10` to the three unguarded multi_lang calls.*

**5. CRITICAL | multi_lang.py:640-644 | Multi-lang crown sleeps 2.5 s minimum, 7.0 s in the normal case**
`await asyncio.sleep(2.5)` is unconditional, and any window with ≥2 competitors adds `sleep(4.5)`. With `FAN_LANGS` = 5 engines the Arabic echo makes ≥2 competitors the default path, so translation does not *begin* until 7 s after `is_final`. (Duo's 0.6 s at line 260 is fine and left alone.)
*Fix: poll the group instead of sleeping in blocks — crown as soon as the expected engine set has reported or 1.2 s elapses, capping the multi window near duo's proven budget.*

**6. CRITICAL | live_mode.py:206-210 | Translation throttle queues up to 20 s, then discards the translation**
`while _tr_inflight >= 3 and waited < 20: await asyncio.sleep(0.3)` — a fast speaker pushes every translation behind a 20-second wait, and the `if _tr_inflight >= 3: return` on line 209 drops it outright with no user-visible signal. The counter is also racy: all waiters wake from the same `sleep(0.3)`, re-read the same sub-3 value, and increment past the cap.
*Fix: replace the counter with `asyncio.Semaphore(3)` and a short `wait_for(sem.acquire(), timeout=2)`, sending an explicit "translation skipped" marker on failure.*

**7. CRITICAL | multi_lang.py:267-269 (duo), 653-656 (multi) | Duplicate guard keys on the wrong text**
`key0 = text_key(items[0][1])` fingerprints the *first arrival* in the window, but the sentence actually emitted is `best`, chosen 100 lines later at line 367/745 (and possibly rewritten by the same-language merge at 344/731). A window whose first competitor is fresh but whose winner is a re-run of a sentence sent 3 s ago passes the guard cleanly and is re-emitted. Amplified by finding 8: when the pulse does fire it repeats every 0.4 s for up to 3 s, so the engine re-issues the same `is_final` up to ~7 times.
*Fix: move the `SENT_AT` scan to immediately after `best` is resolved and run it against `text_key(text)`, not `items[0]`.*

**8. CRITICAL | live_mode.py:337-348 | Dedup is keyed on fragments, never on the sentence actually sent**
`_recent_sent` stores each accepted fragment, but the user receives `full_sentence = " ".join(_fragments)` (line 190), which is never recorded or checked. The standard STT pattern — fragment finals followed by a consolidated final after silence — therefore re-emits the whole sentence as a fresh one, since the consolidated form matches no single stored fragment. Two secondary bypasses in the same block: `_norm` (line 337) lowercases and collapses whitespace but keeps punctuation, so `"…investir"` vs `"…investir."` compare unequal; and the substring rule is gated to `_wc <= 4`, leaving every sentence over four words on literal equality alone.
*Fix: record `_norm(full_sentence)` into `_recent_sent` inside `_flush_fragments` and test against it there, using a punctuation-stripped key (reuse `text_key`).*

**9. HIGH | multi_lang.py:759-772 | Second-pass emission bypasses the primary duplicate guard**
The primary guard is a 25-char-prefix scan over `SENT_AT` (line 654); the second pass uses exact full-key membership, `text_key(t) not in SENT_AT`. A near-duplicate diverging after character 25, or a prefix of an already-sent sentence, fails the exact test and is emitted. `second` also skips `_is_orphan_tail` entirely, the one filter written to catch exactly this shape of leftover.
*Fix: route `second` through the same prefix scan and `_is_orphan_tail(t2, now)` check before sending.*

**10. HIGH | multi_lang.py:191,554 vs 373,751 | `_recent_crowned` is populated too late and evicts too fast to catch echoes**
`_is_orphan_tail` reads `_recent_crowned`, but entries are only appended in `_crown` — 0.6 s after arrival in duo and **2.5–7.0 s** in multi. Every echo landing inside that gap tests against a history that does not yet contain the sentence it echoes. The 8-entry cap (lines 374, 752) compounds it: with 5 fan engines a single busy window can evict the 12 s history before the late echo arrives. The `>4 words → return False` early-out (line 189/552) also lets any longer repeat through untouched.
*Fix: append to `_recent_crowned` when the winner is selected and raise the cap to ~32 (time-pruned at 12 s), so the window is bounded by time rather than by count.*

---

**Also noted, below the top 10:** duo's echo blocker (multi_lang.py:362-366) requires `len(t.split()) <= 1` **and** `t.isascii()` **and** an `ar` competitor — a two-word echo, an accented one, or any en↔fr pair bypasses it entirely; multi's `collector` still recurses via `await collector(lang, new_up)` (multi_lang.py:846) where duo was fixed to `create_task`, and multi's pump reopen path (line 599-601) installs a new upstream without a collector, leaving that engine deaf until the 20 s+15 s guard notices; `SILENCE_1S` (multi_lang.py:28) is 8000 samples = **500 ms**, not the 1 s the name and comment claim — still above `endpointing=300`, but the margin is half what it looks like.

**AUDIT_DONE**