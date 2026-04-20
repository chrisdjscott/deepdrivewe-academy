# Bug Analysis: Timeouts and Deadlocks in openmm_ntl9_hk runs

Slurm jobs 5528197–5543613, run on Mahuika GPU nodes (g09/g11), 2026-04-15/16.
Three distinct bugs were diagnosed from the output files and fixed in commits
`a3aa308`, `415e27a`, and `b37bef6` on the `example-deadlocks` branch.

---

## Bug 1 — Duplicate simulation IDs from unresolved resampler index counter

**Fixed by:** `a3aa308` — `deepdrivewe/resamplers/base.py`

**Observed in:** `slurm-5529488`

**Error:**
```
ERROR (academy.runtime) Error in loop 'run_westpa' (signaling shutdown: True)
FileNotFoundError: [Errno 2] No such file or directory:
  '.../results/simulation/000011/000059/seg.pdb'
```
Followed by a cascade of `asyncio.exceptions.CancelledError` and
`RuntimeError: Session is closed` during shutdown.

**Root cause:** `_get_next_sims` assigns `simulation_id = 0..N-1` via
`enumerate`, but `_index_counter` was never reset to `N` afterward. Any
walkers created by split/merge in the same `run()` call restarted counting
from 0, producing duplicate IDs that collided with the continuation walkers
and pointed to paths that did not exist on disk.

**Fix:** Reset `_index_counter` to `itertools.count(len(next_sims))` after
`_get_next_sims` so split/merge walkers always receive IDs ≥ N.

---

## Bug 2 — Deadlock when walker count exceeds available GPU slots

**Fixed by:** `415e27a` — `deepdrivewe/workflows/westpa.py`,
`examples/openmm_ntl9_hk/config.yaml`, `main.py`, `workflow.py`

**Observed in:** `slurm-5540615`

**Error:** No exception — the workflow silently stalled. The log shows 72
Parsl tasks dispatched immediately (one agent per walker), only 4 simulation
results ever returned, then Slurm SIGTERM'd the job after hitting wall time:

```
Parsl task 0 try 0 launched on executor htex with executor id 1
Parsl task 1 try 0 launched on executor htex with executor id 2
...
Parsl task 71 try 0 launched on executor htex with executor id 72
...
received sim 1 iter 11. batch: 1/72
received sim 0 iter 11. batch: 2/72
received sim 4 iter 11. batch: 3/72
received sim 3 iter 11. batch: 4/72
*** JOB 5540615 ON g11 CANCELLED AT 2026-04-15T20:36:35 DUE to SIGNAL Terminated ***
```

**Root cause:** The workflow launched one `SimulationAgent` per walker. When
the walker count exceeded the number of available GPU slots, agents beyond the
slot limit queued indefinitely waiting for a slot. The orchestrator waited for
results from all agents, creating a circular block — agents could not start
without a slot, and the orchestrator could not proceed without results.

**Fix:** Add a `num_sim_agents` parameter to `run_westpa_workflow` and
`ExperimentSettings` so the agent pool is sized to match available GPU slots
(independent of walker count). Also added a semaphore to limit concurrent SSE
dispatches and exponential-backoff retries in `dispatch_round_robin` to handle
transient connection-pool exhaustion under high walker counts. As a partial
mitigation, aiohttp `total`/`sock_read` timeouts are set to `None` at the
workflow level.

---

## Bug 3 — SSE listener TimeoutError in Parsl worker sim agents after 300 s

**Fixed by:** `b37bef6` — `deepdrivewe/workflows/westpa.py`, `pyproject.toml`

**Observed in:** `slurm-5528681` (iteration 11, 72 walkers) and
`slurm-5540646` (iteration 17, 88 walkers)

**Error (identical traceback in both files):**
```
ERROR (academy.logging) Background task raised an exception.
  File ".../aiohttp/streams.py", line 372, in _wait
    await waiter
asyncio.exceptions.CancelledError

The above exception was the direct cause of the following exception:

  File ".../academy/logging.py", line 213, in execute_and_log_traceback
    return await fut
  File ".../academy/exchange/client.py", line 224, in _listen_for_messages
    async for message in self._transport.listen():
  File ".../academy/exchange/cloud/client.py", line 252, in listen
    async for line_in_bytes in response.content:
  File ".../aiohttp/streams.py", line 53, in __anext__
  File ".../aiohttp/streams.py", line 377, in readline
  File ".../aiohttp/streams.py", line 414, in readuntil
    await self._wait("readuntil")
  File ".../aiohttp/streams.py", line 371, in _wait
    with self._timer:
  File ".../aiohttp/helpers.py", line 713, in __exit__
    raise asyncio.TimeoutError from exc_val
TimeoutError
```

**Timing pattern:** In both cases the `TimeoutError` fired exactly ~300 seconds
after the Parsl worker sim agents established their SSE connections — precisely
aiohttp's default `total=300s` timeout. After the timeout, agents went deaf to
all further messages; the workflow appeared to hang until Slurm SIGTERM'd it.

- `slurm-5528681`: agents dispatching iter 11 at `21:13:48`, TimeoutError at
  `21:14:11` (300 s after workers launched ~`21:08`).
- `slurm-5540646`: agents dispatching iter 17 at `08:42:52`, TimeoutError at
  `08:42:58` (300 s after workers launched ~`08:37`).

**Root cause:** `main.py` patched `aiohttp.DEFAULT_TIMEOUT` to `None`, but
Parsl sim agents run as fresh `process_worker_pool.py` subprocesses that never
execute `main.py`. Those workers therefore inherited aiohttp's default
`total=300s`, and the Academy SSE listener background task raised `TimeoutError`
after exactly 5 minutes, making agents permanently deaf to subsequent simulation
tasks.

**Fix:** Set `DEFAULT_TIMEOUT` in `SimulationAgent.__init__`, which runs on
each worker before Academy's `run_until_complete()` creates the `ClientSession`.
Also added `aiohttp` as an explicit direct dependency in `pyproject.toml`.

---

## Run progression summary

| Slurm job | Date/time (NZST) | Outcome |
|---|---|---|
| 5528197 | 2026-04-15 20:49 | `PicklingError: LocalExchangeFactory is not pickleable` — no sims ran |
| 5528252 | 2026-04-15 | `ValidationError: 6 validation errors for ExperimentSettings` — config mismatch |
| 5528292 | 2026-04-15 20:54 | Same pickling error |
| 5528460 | 2026-04-15 | Silent failure at startup |
| 5528681 | 2026-04-15 21:14 | SSE TimeoutError at 300 s (iter 11, 72 walkers) |
| 5529488 | 2026-04-15 21:41 | `FileNotFoundError` from duplicate sim IDs |
| 5529655 | 2026-04-15 | Silent failure |
| 5540615 | 2026-04-16 08:14 | Silent deadlock — 72 agents queued, only 4 results, hung |
| 5540646 | 2026-04-16 08:42 | SSE TimeoutError at 300 s (iter 17, 88 walkers) |
| 5540677 | 2026-04-16 09:31 | Reached iter 36 (196 walkers), hit wall time cleanly |
| 5541657 | 2026-04-16 10:25 | Continuation from iter 39, hit wall time |
| 5541910 | 2026-04-16 10:43 | Continuation from iter 42, hit wall time |
| 5541984 | 2026-04-16 11:01 | Continuation from iter 45, hit wall time |
| 5542765 | 2026-04-16 12:37 | Continuation from iter 48, hit wall time |
| **5543613** | **2026-04-16 13:05** | **Clean shutdown — Parsl DFK teardown + `Closed manager` logged** |

---

## Plan: back-port fixes to individual branches for GitHub issues and PRs

All three fix commits currently live only on `example-deadlocks`. The goal is
to land each fix on `main` (via `develop`) as a separate, reviewable PR with a
corresponding GitHub issue.

### Dependency note

Commits `415e27a` (deadlock) and `b37bef6` (SSE timeout) both modify
`deepdrivewe/workflows/westpa.py`. `b37bef6` was authored on top of `415e27a`,
so cherry-picking `b37bef6` onto a branch that does not include `415e27a` will
likely conflict in `westpa.py`. The recommended approach is:

- Cherry-pick `a3aa308` (resampler) independently — no conflicts expected.
- Cherry-pick `415e27a` (deadlock) independently — touches `westpa.py` and
  example files; clean against `main`.
- Cherry-pick `b37bef6` (SSE timeout) onto a branch that already includes the
  `415e27a` `westpa.py` hunk, **or** resolve the conflict manually by isolating
  only the `SimulationAgent.__init__` change.

---

### Step-by-step

#### 0. Prerequisites

Ensure `main` and `develop` are up to date locally:

```bash
git fetch origin
git checkout develop && git pull origin develop
git checkout main && git pull origin main
```

---

#### Fix 1 — Resampler duplicate simulation IDs

**GitHub issue title:** `Resampler assigns duplicate simulation IDs to split/merge walkers`

**Issue body:** After `_get_next_sims` assigns IDs 0..N-1 to continuation
walkers via `enumerate`, `_index_counter` is not reset. Walkers created by
split/merge in the same `run()` call restart from 0, colliding with continuation
walker IDs and causing `FileNotFoundError` for trajectory files that belong to
the wrong iteration directory. Observed in `slurm-5529488`.

**Branch and PR:**

```bash
git checkout develop
git checkout -b bugfix/<issue>-resampler-duplicate-sim-ids

git cherry-pick a3aa308

git push -u origin bugfix/<issue>-resampler-duplicate-sim-ids
# Open PR targeting develop
```

Files changed: `deepdrivewe/resamplers/base.py` (+6 lines)

No conflicts expected — this file is untouched by the other two fixes.

---

#### Fix 2 — Deadlock when walkers exceed GPU slot capacity

**GitHub issue title:** `Workflow deadlocks when walker count exceeds available GPU slots`

**Issue body:** `run_westpa_workflow` launches one `SimulationAgent` per
walker. When the resampler grows the walker population beyond the number of
available GPU slots, excess agents queue indefinitely waiting for a slot while
the orchestrator waits for results from all of them — a circular block. The
workflow silently hangs with no error until the Slurm wall time is reached.
Observed in `slurm-5540615` (72 walkers, 4 results returned).

**Branch and PR:**

```bash
git checkout develop
git checkout -b bugfix/<issue>-deadlock-walker-gpu-slots

git cherry-pick 415e27a

git push -u origin bugfix/<issue>-deadlock-walker-gpu-slots
# Open PR targeting develop
```

Files changed:
- `deepdrivewe/workflows/westpa.py` (+39/-11)
- `examples/openmm_ntl9_hk/config.yaml` (+5)
- `examples/openmm_ntl9_hk/main.py` (+21)
- `examples/openmm_ntl9_hk/workflow.py` (+10)

No conflicts expected against a clean `develop`/`main` base.

---

#### Fix 3 — SSE listener timeout in Parsl worker sim agents

**GitHub issue title:** `SimulationAgent goes deaf after 300 s due to aiohttp default timeout in Parsl workers`

**Issue body:** `main.py` patches `aiohttp.DEFAULT_TIMEOUT` to `None` to
prevent the Academy SSE listener from timing out during long-running simulations.
However, Parsl sim agents run as fresh `process_worker_pool.py` subprocesses
that never execute `main.py`, so the patch is invisible to them. Workers inherit
aiohttp's default `total=300s`; the Academy SSE background task raises
`TimeoutError` after exactly 5 minutes, making agents permanently deaf to further
simulation tasks. Observed in `slurm-5528681` and `slurm-5540646`.

**Branch and PR:**

```bash
git checkout develop
git checkout -b bugfix/<issue>-sim-agent-aiohttp-timeout

# Option A: cherry-pick cleanly if Fix 2 PR has already merged to develop
git cherry-pick b37bef6

# Option B: if cherry-pick conflicts on westpa.py (because 415e27a is not yet
# in the base), resolve manually — the only change needed in westpa.py from
# b37bef6 is the SimulationAgent.__init__ block that sets DEFAULT_TIMEOUT.
# The pyproject.toml change (adding aiohttp dependency) will apply cleanly.

git push -u origin bugfix/<issue>-sim-agent-aiohttp-timeout
# Open PR targeting develop
```

Files changed:
- `deepdrivewe/workflows/westpa.py` (+17)
- `pyproject.toml` (+1)

**Recommended order:** Open the Fix 2 PR first and merge it before opening Fix 3,
so the `westpa.py` base is consistent and `b37bef6` cherry-picks without conflict.
Alternatively, base the Fix 3 branch on the Fix 2 branch and stack the PRs.

---

### Suggested PR descriptions

Each PR should reference its issue and include:

- A one-paragraph description of the symptom and root cause.
- The relevant error message or log excerpt from the slurm output files above.
- A test plan noting that the fix can be verified by running the
  `openmm_ntl9_hk` example and confirming:
  - Fix 1: no `FileNotFoundError` for trajectory files after resampling.
  - Fix 2: workflow runs to completion when `num_sim_agents` is set ≤ GPU slots.
  - Fix 3: simulation agents remain responsive past the 5-minute mark.
