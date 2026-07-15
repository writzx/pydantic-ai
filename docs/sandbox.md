# Sandboxes

Agents that write files and run commands need somewhere to do it that isn't your machine's bare filesystem and shell. A **sandbox** is an isolated execution environment — a subprocess jail, a container, a microVM, a remote worker — attached to an agent run: tools and capabilities reach it through the readonly [`RunContext.sandbox`][pydantic_ai.tools.RunContext.sandbox] field, execute commands in it, and read and write its files.

Pydantic AI defines the **concept, not an implementation**: [`Sandbox`][pydantic_ai.sandbox.Sandbox] is a [structural protocol](https://typing.python.org/en/latest/spec/protocol.html), so any object with the right surface conforms — no base class, no registration, no dependency on any particular sandboxing library. Wire up a Docker container or a cloud sandbox SDK — or start with the shipped, isolation-free [`LocalSandbox`][pydantic_ai.local_sandbox.LocalSandbox] ([below](#a-minimal-local-implementation)).

## The protocol

A sandbox exposes two things: **command execution** and **file access**.

```python {test="skip"}
from pydantic_ai.sandbox import Sandbox


async def analyze(sandbox: Sandbox) -> str:
    await sandbox.fs.write_text('/workspace/count.py', 'print(sum(range(10)))')
    result = await sandbox.run(['python', '/workspace/count.py'], timeout=30)
    if result.exit_code != 0:
        return f'failed: {result.stderr}'
    return result.stdout
```

| Member | What it does |
|---|---|
| [`run(command, ...)`][pydantic_ai.sandbox.Sandbox.run] | Execute an argv sequence (or, with `shell=True`, a shell string) and wait for the [result][pydantic_ai.sandbox.SandboxResult]. |
| [`start(command, ...)`][pydantic_ai.sandbox.Sandbox.start] | Start a command and return a [`SandboxProcess`][pydantic_ai.sandbox.SandboxProcess] with `wait()`, `stream()`, and `kill()`. |
| [`fs`][pydantic_ai.sandbox.Sandbox.fs] | A [`SandboxFilesystem`][pydantic_ai.sandbox.SandboxFilesystem]: `read_bytes`/`write_bytes`, `read_text`/`write_text`, `stat`, `list_dir`, `make_dir`, `remove`, `exists`. |
| [`working_dir()`][pydantic_ai.sandbox.Sandbox.working_dir] / [`resolve(path)`][pydantic_ai.sandbox.Sandbox.resolve] | The default working directory, and a helper to make model-supplied relative paths absolute. |
| [`provider`][pydantic_ai.sandbox.Sandbox.provider] / [`sandbox_id`][pydantic_ai.sandbox.Sandbox.sandbox_id] | Identity for logs and serialized references. |

Three contracts to know when writing code against the protocol (implementers: see the [API reference][pydantic_ai.sandbox] for the full set):

- **Optional operations raise `NotImplementedError`.** Not every backend can stream output, kill a process, or bound retained output (`output_limit=`). Treat `NotImplementedError` as "use the fallback": `wait()` instead of `stream()`, `timeout=` instead of `kill()`.
- **`timeout=` is a kill guarantee** — the command is terminated and an exception deriving from `TimeoutError` is raised. Merely cancelling the awaiting task is *not* guaranteed to stop the remote command.
- **Results are honest.** A non-zero `exit_code` is a normal result, not an exception; check it.

!!! warning "A sandbox protocol is not a security boundary"
    Isolation comes from the environment the implementation provides (the container, the VM, the jail) — not from this interface. In particular [`resolve()`][pydantic_ai.sandbox.Sandbox.resolve] is a textual path convenience: `..` can escape the base directory and symlinks are not inspected. If you need path confinement, enforce it in the sandbox itself.

## Attaching a sandbox to a run

There are two routes, mirroring how [model settings](agents.md#model-run-settings) layer:

**1. Directly, per run** — you create it, you own it:

```python {test="skip" lint="skip"}
async with make_docker_sandbox() as sandbox:  # any Sandbox-conforming object
    result = await agent.run('Profile the script and fix the hot spot.', sandbox=sandbox)
```

**2. From a capability** — the capability manages acquisition and teardown via the [`get_sandbox`][pydantic_ai.capabilities.AbstractCapability.get_sandbox] hook, so agent code stays a one-liner:

```python {test="skip" lint="skip"}
agent = Agent('anthropic:claude-sonnet-5', capabilities=[MySandboxCapability()])
```

A sandbox passed to the run method **wins** over anything a capability would contribute — the capability hooks are then never invoked, exactly like run-level `model_settings` beating capability settings. Among capabilities, the one **latest in the resolved chain** wins (with no [ordering constraints][pydantic_ai.capabilities.CapabilityOrdering] in play, that's the last one registered); earlier capabilities are only consulted if it returns `None`, and deferred capabilities that haven't been loaded never contribute. When more than one capability overrides `get_sandbox`, Pydantic AI emits a `UserWarning` naming this winner rule — a silently shadowed sandbox is a debugging trap.

## Lifecycle: whoever supplies it, owns it

Pydantic AI **never creates, enters, closes, or destroys a sandbox**. It only carries the reference for the duration of the run:

- `run(..., sandbox=...)`: the caller owns it. Create it before the run, tear it down after (typically an `async with` around the run).
- `get_sandbox`: the contributing capability owns it. The hook fires *inside the run body*, after every capability's [`wrap_run`][pydantic_ai.capabilities.AbstractCapability.wrap_run] has entered — so the canonical per-run pattern acquires in `get_sandbox` and tears down in `wrap_run`'s `finally`:

```python {title="sandbox_capability.py" test="skip"}
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from typing import Any

from my_sandboxes import make_docker_sandbox  # your factory

from pydantic_ai import RunContext
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.run import AgentRunResult
from pydantic_ai.sandbox import Sandbox


@dataclass
class MySandboxCapability(AbstractCapability[Any]):
    _stack: AsyncExitStack | None = field(default=None, init=False, repr=False)

    async def for_run(self, ctx: RunContext[Any]) -> AbstractCapability[Any]:
        return replace(self)  # fresh copy per run: isolates the handle under concurrency

    async def get_sandbox(self, ctx: RunContext[Any]) -> Sandbox | None:
        # Called at most once per run, and only if this capability is selected —
        # a "losing" capability never pays the acquisition cost.
        self._stack = AsyncExitStack()
        return await self._stack.enter_async_context(make_docker_sandbox())

    async def wrap_run(
        self, ctx: RunContext[Any], *, handler: Callable[[], Awaitable[AgentRunResult[Any]]]
    ) -> AgentRunResult[Any]:
        try:
            return await handler()
        finally:
            if self._stack is not None:  # never acquired -> nothing to clean up
                await self._stack.aclose()
```

A capability that shares one **warm sandbox across runs** (state persists between conversations) skips the `wrap_run` teardown entirely and just returns its long-lived handle from `get_sandbox` — nothing will touch it at run end.

!!! warning "`for_run` isolation is not optional for owned sandboxes"
    The `replace(self)` in `for_run` above is load-bearing: without it, two concurrent runs share one capability instance, and one run's teardown destroys the sandbox the other run is still using (while the first-acquired handle leaks). If your capability owns per-run state of any kind, return a fresh copy from `for_run`.

### Availability

Where `ctx.sandbox` is non-`None` depends on who supplied it:

| Phase | `run(sandbox=...)` | capability-contributed |
|---|---|---|
| `for_run`, initial metadata factories | ✅ | ❌ |
| `wrap_run` before `await handler()` | ✅ | ❌ (resolved inside the handler) |
| `before_run`, instructions, **tool calls**, output validation, event streams | ✅ | ✅ |
| streamed-result consumption (`run_stream` body) | ✅ | ✅ |
| `after_run`, `on_run_error`, toolset `__aexit__` | ✅ (caller-owned, still live) | ❌ (cleared) |

A capability-contributed sandbox is **cleared from the context when the run body finishes**, because its owner may destroy it the moment the wrap chain unwinds — hooks that run later see `None` instead of a live-looking dead handle. A toolset that needs the sandbox during its own `__aexit__` should capture the handle during the run. One nuance: metadata factories are evaluated twice, and the *final* re-evaluation (when the run produces a result, just before the run body finishes) still observes a capability-contributed sandbox — only the initial evaluation predates it.

## Durable execution

A live sandbox handle **does not survive a durable-execution boundary** — this is inherent, not an implementation gap:

- **[Temporal](durable_execution/temporal.md)**: tool bodies run in activities, where `RunContext` is rebuilt from a serialized allowlist; a live handle can't cross, and capability hooks run as workflow code where creating a sandbox (network I/O) is forbidden. Temporal agents therefore **reject** both `run(sandbox=...)` and sandbox-contributing capabilities inside a workflow with a clear error, and `ctx.sandbox` inside an activity raises [`UserError`][pydantic_ai.exceptions.UserError] instead of silently returning `None`. One caveat: the capability check is static, so a *dynamic* capability whose factory only resolves to a sandbox contributor at run time cannot be detected up front — its `get_sandbox` would then execute as workflow code and fail (or worse, break replay determinism) at I/O time. Don't contribute sandboxes from dynamic capabilities in Temporal workflows.
- **[DBOS](durable_execution/dbos.md)**: run arguments are pickled as workflow inputs, so `run(sandbox=...)` is rejected. Function tools run inline in workflow code, so a *capability-contributed* sandbox does work on the happy path — but on crash recovery the workflow re-executes from the top: the capability acquires a **second** sandbox, the first one's teardown never ran (leaked — give sandboxes a server-side TTL), and inline tools re-execute their side effects against the fresh sandbox while recorded steps replay old results. Prefer a shared sandbox keyed by a stable id (e.g. the DBOS workflow id) over owned-per-run.
- **[Prefect](durable_execution/prefect.md)**: tool calls are tasks with input-hash caching; the sandbox's provider-qualified identity (`provider` + `sandbox_id` — ids are only unique within a provider) participates in the cache key so a flow-run retry with a fresh sandbox can't silently replay results recorded against a dead one.

The pattern that *does* work everywhere: carry a **serializable reference** and re-open the sandbox where the work happens.

```python {title="durable_sandbox_pattern.py" test="skip"}
from dataclasses import dataclass

from my_sandboxes import open_sandbox  # worker-side factory holding the credentials

from pydantic_ai import Agent, RunContext


@dataclass
class SandboxRef:
    provider: str
    sandbox_id: str  # ids only — keep credentials worker-side, out of workflow history


agent = Agent('anthropic:claude-sonnet-5', deps_type=SandboxRef)


@agent.tool
async def sh(ctx: RunContext[SandboxRef], command: str) -> str:
    # Runs activity-side: re-open by id using your implementation's reconnection API.
    sandbox = await open_sandbox(ctx.deps.provider, ctx.deps.sandbox_id)
    result = await sandbox.run(command, shell=True, timeout=60)
    return result.stdout if result.exit_code == 0 else f'[exit {result.exit_code}] {result.stderr}'
```

Rules of thumb for the reference pattern:

- **Create the sandbox in an activity** (or before the workflow starts), keyed idempotently (e.g. on the workflow id) so an activity retry can't create duplicates.
- **Destroy in a workflow `finally` — and still set a server-side TTL.** A terminated workflow runs no cleanup; without a TTL/reaper, that's a guaranteed leak.
- **Ids only in `deps`/`metadata`.** Both are serialized into every activity payload and recorded in workflow history; credentials belong in worker-side configuration, mirroring the Temporal `provider_factory` pattern.
- **Fail loudly on expiry.** If the sandbox was reaped while the workflow slept, an open-*or-create* fallback silently swaps in an empty environment that the model's message history contradicts. Recreate only as an explicit, logged decision.

Re-opening by `sandbox_id` inside each tool call is exactly why the protocol requires `provider` and `sandbox_id` — they are the durable half of an otherwise live-only object. First-class rehydration inside the durable integrations (a worker-side sandbox factory, sandbox creation as a managed activity) is planned as a follow-up.

## A minimal local implementation

Pydantic AI ships one batteries-included implementation, [`LocalSandbox`][pydantic_ai.local_sandbox.LocalSandbox]: host subprocesses and the host filesystem behind the protocol surface. It **isolates nothing** — use it for trusted workloads, tests, and development, and swap in a real sandbox for anything else. POSIX only; construction raises `NotImplementedError` elsewhere:

```python {test="skip"}
from pydantic_ai import Agent, LocalSandbox

agent = Agent('anthropic:claude-sonnet-5')


async def main() -> None:
    async with LocalSandbox() as sandbox:  # a temp directory, removed on exit
        await agent.run('Write fizzbuzz to fizzbuzz.py and run it.', sandbox=sandbox)
```

It is also the reference for implementing the protocol yourself: the floor is deliberately small — a local implementation over `asyncio.subprocess` and `pathlib` fits on a page.

```python {title="local_sandbox.py" test="skip"}
import asyncio
import posixpath
import shutil
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LocalResult:
    exit_code: int
    stdout: str
    stderr: str
    stdout_dropped: int = 0
    stderr_dropped: int = 0


@dataclass(frozen=True)
class LocalFileEntry:
    name: str
    path: str
    is_dir: bool
    size: int | None


class LocalFilesystem:
    def __init__(self, root: Path):
        self._root = root

    async def read_bytes(self, path: str) -> bytes:
        return await asyncio.to_thread(Path(path).read_bytes)

    async def write_bytes(self, path: str, data: bytes) -> None:
        def write() -> None:
            Path(path).parent.mkdir(parents=True, exist_ok=True)  # the write contract creates parents
            Path(path).write_bytes(data)

        await asyncio.to_thread(write)

    async def read_text(self, path: str, encoding: str = 'utf-8') -> str:
        return (await self.read_bytes(path)).decode(encoding)

    async def write_text(self, path: str, content: str, encoding: str = 'utf-8') -> None:
        await self.write_bytes(path, content.encode(encoding))

    async def stat(self, path: str) -> LocalFileEntry:
        p = Path(path)
        st = await asyncio.to_thread(p.stat)
        return LocalFileEntry(p.name, path, p.is_dir(), None if p.is_dir() else st.st_size)

    async def list_dir(self, path: str) -> list[LocalFileEntry]:
        return [await self.stat(str(child)) for child in sorted(Path(path).iterdir())]

    async def make_dir(self, path: str) -> None:
        await asyncio.to_thread(Path(path).mkdir, parents=True, exist_ok=True)

    async def remove(self, path: str) -> None:
        p = Path(path)
        await asyncio.to_thread(shutil.rmtree, p) if p.is_dir() else await asyncio.to_thread(p.unlink)

    async def exists(self, path: str) -> bool:
        return await asyncio.to_thread(Path(path).exists)


class LocalSandbox:
    provider = 'local'

    def __init__(self, root: Path):
        self._root = root
        self._id = f'local-{uuid.uuid4().hex[:8]}'
        self.fs = LocalFilesystem(root)

    @property
    def sandbox_id(self) -> str:
        return self._id

    async def working_dir(self) -> str:
        return str(self._root)

    async def resolve(self, path: str, *, base: str | None = None) -> str:
        if posixpath.isabs(path):
            return posixpath.normpath(path)
        return posixpath.normpath(posixpath.join(base or str(self._root), path))

    async def run(
        self,
        command: str | Sequence[str],
        *,
        shell: bool = False,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        output_limit: int | None = None,
    ) -> LocalResult:
        if output_limit is not None:
            raise NotImplementedError('LocalSandbox does not bound output; bound it in-command, e.g. `| tail -c`.')
        if shell:
            assert isinstance(command, str)
            proc = await asyncio.create_subprocess_shell(
                command, cwd=cwd or self._root, env=dict(env) if env else None,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
        else:
            assert not isinstance(command, str), 'string commands require shell=True'
            proc = await asyncio.create_subprocess_exec(
                *command, cwd=cwd or self._root, env=dict(env) if env else None,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        except (TimeoutError, asyncio.TimeoutError) as e:  # asyncio's is distinct on 3.10
            proc.kill()  # the contract: a timeout kills the command...
            await proc.wait()
            raise TimeoutError(f'command timed out after {timeout} seconds') from e  # ...as a builtin TimeoutError
        return LocalResult(proc.returncode or 0, stdout.decode(), stderr.decode())

    async def start(self, command, **kwargs):
        raise NotImplementedError('LocalSandbox only supports run(); use timeout= to bound commands.')
```

Verify conformance statically — this is the whole "registration" story:

```python {test="skip" requires="local_sandbox.py"}
from pathlib import Path

from pydantic_ai.sandbox import Sandbox

from local_sandbox import LocalSandbox

sandbox: Sandbox = LocalSandbox(Path('/tmp/agent-workspace'))  # type-checked structurally
```

## Building tools on the sandbox

The framework attaches the sandbox but ships no sandbox tools: what to expose (`execute`? `read_file`? approval gates? output truncation policy?) is an application decision. A tool reads `ctx.sandbox` and goes:

```python {test="skip"}
from pydantic_ai import Agent, RunContext

agent = Agent('anthropic:claude-sonnet-5')


@agent.tool
async def execute(ctx: RunContext[None], command: str, timeout: float = 30.0) -> str:
    """Run a shell command in the workspace."""
    sandbox = ctx.sandbox
    assert sandbox is not None, 'attach a sandbox to the run or register a sandbox capability'
    result = await sandbox.run(command, shell=True, timeout=min(timeout, 120.0))
    output = result.stdout + (f'\n[stderr]\n{result.stderr}' if result.stderr else '')
    return output if result.exit_code == 0 else f'[exit code: {result.exit_code}]\n{output}'
```

Keep policy (allow/deny lists, path rules, output budgets) in the tool layer; keep isolation in the sandbox. Denylists over free-form shell strings are security theater — if commands must be constrained, use argv form and validate arguments.
