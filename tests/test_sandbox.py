"""Tests for the sandbox concept: the `Sandbox` protocol, the readonly `RunContext.sandbox`
field, the `get_sandbox` capability hook, and their precedence and lifecycle semantics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import pytest

from pydantic_ai import Agent, RunContext
from pydantic_ai.capabilities import AbstractCapability, CapabilityOrdering, WrapperCapability
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.run import AgentRunResult
from pydantic_ai.sandbox import Sandbox
from pydantic_ai.toolsets import FunctionToolset, WrapperToolset
from pydantic_ai.usage import RunUsage

pytestmark = pytest.mark.anyio


@dataclass(frozen=True)
class _Result:
    exit_code: int
    stdout: str
    stderr: str
    stdout_dropped: int = 0
    stderr_dropped: int = 0


@dataclass(frozen=True)
class _Entry:
    name: str
    path: str
    is_dir: bool
    size: int | None = None


class _Fs:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    async def read_bytes(self, path: str) -> bytes:
        return self.files[path]

    async def write_bytes(self, path: str, data: bytes) -> None:
        self.files[path] = data

    async def read_text(self, path: str, encoding: str = 'utf-8') -> str:
        return self.files[path].decode(encoding)

    async def write_text(self, path: str, content: str, encoding: str = 'utf-8') -> None:
        self.files[path] = content.encode(encoding)

    async def stat(self, path: str) -> _Entry:
        return _Entry(name=path.rsplit('/', 1)[-1], path=path, is_dir=False, size=len(self.files[path]))

    async def list_dir(self, path: str) -> Sequence[_Entry]:
        return [await self.stat(p) for p in self.files]

    async def make_dir(self, path: str) -> None:
        pass

    async def remove(self, path: str) -> None:
        self.files.pop(path, None)

    async def exists(self, path: str) -> bool:
        return path in self.files


class FakeSandbox:
    """A minimal in-memory implementation of the `Sandbox` protocol."""

    provider = 'fake'

    def __init__(self, name: str) -> None:
        self.name = name
        self.destroyed = False
        self._fs = _Fs()

    @property
    def sandbox_id(self) -> str:
        return f'fake-{self.name}'

    @property
    def fs(self) -> _Fs:
        return self._fs

    async def run(
        self,
        command: str | Sequence[str],
        *,
        shell: bool = False,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        output_limit: int | None = None,
    ) -> _Result:
        assert not self.destroyed, 'command executed on a destroyed sandbox'
        return _Result(exit_code=0, stdout=f'ran:{command}', stderr='')

    async def start(
        self,
        command: str | Sequence[str],
        *,
        shell: bool = False,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        output_limit: int | None = None,
    ) -> Any:
        raise NotImplementedError('FakeSandbox cannot start background processes; use `run` instead.')

    async def working_dir(self) -> str:
        return '/workspace'

    async def resolve(self, path: str, *, base: str | None = None) -> str:
        if path.startswith('/'):
            return path
        return f'{base or "/workspace"}/{path}'


@dataclass
class SandboxCapability(AbstractCapability[Any]):
    """Canonical per-run sandbox capability: acquire in `get_sandbox`, tear down in `wrap_run`."""

    name: str = 'sandbox'
    log: list[str] = field(default_factory=lambda: [])
    acquired: FakeSandbox | None = field(default=None, init=False, repr=False)

    async def for_run(self, ctx: RunContext[Any]) -> AbstractCapability[Any]:
        return replace(self)

    async def get_sandbox(self, ctx: RunContext[Any]) -> Sandbox | None:
        self.log.append(f'{self.name}:get_sandbox')
        self.acquired = FakeSandbox(self.name)
        return self.acquired

    async def wrap_run(self, ctx: RunContext[Any], *, handler: Any) -> AgentRunResult[Any]:
        self.log.append(f'{self.name}:wrap_enter:{_describe(ctx.sandbox)}')
        try:
            return await handler()
        finally:
            if self.acquired is not None:
                self.acquired.destroyed = True
            self.log.append(f'{self.name}:wrap_exit')

    async def before_run(self, ctx: RunContext[Any]) -> None:
        self.log.append(f'{self.name}:before_run:{_describe(ctx.sandbox)}')

    async def after_run(self, ctx: RunContext[Any], *, result: AgentRunResult[Any]) -> AgentRunResult[Any]:
        self.log.append(f'{self.name}:after_run:{_describe(ctx.sandbox)}')
        return result

    async def on_run_error(self, ctx: RunContext[Any], *, error: BaseException) -> AgentRunResult[Any]:
        self.log.append(f'{self.name}:on_run_error:{_describe(ctx.sandbox)}')
        raise error


def _describe(sandbox: Sandbox | None) -> str:
    if sandbox is None:
        return 'none'
    return getattr(sandbox, 'name', sandbox.sandbox_id)


def _tool_call_then_text(tool_name: str = 'probe') -> FunctionModel:
    """A model that calls `tool_name` on the first step and returns text on the second."""

    def model_func(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart(tool_name, {})])
        return ModelResponse(parts=[TextPart('done')])

    return FunctionModel(model_func)


def make_probe_agent(seen: list[str], **kwargs: Any) -> Agent:
    agent: Agent = Agent(_tool_call_then_text(), **kwargs)

    @agent.tool
    async def probe(ctx: RunContext[Any]) -> str:
        seen.append(_describe(ctx.sandbox))
        return 'ok'

    return agent


def test_fake_sandbox_conforms_to_protocol():
    sandbox = FakeSandbox('x')
    assert isinstance(sandbox, Sandbox)
    # Static conformance too: pyright checks this assignment because tests are type-checked.
    typed: Sandbox = sandbox
    assert typed.provider == 'fake'


def test_run_context_sandbox_is_readonly():
    assert isinstance(RunContext.sandbox, property)
    assert RunContext.sandbox.fset is None
    ctx = RunContext[None](deps=None, model=TestModel(), usage=RunUsage())
    with pytest.raises(AttributeError):
        ctx.sandbox = FakeSandbox('nope')  # type: ignore[misc]


async def test_run_argument_sandbox_reaches_tools():
    seen: list[str] = []
    agent = make_probe_agent(seen)
    sandbox = FakeSandbox('direct')
    result = await agent.run('go', sandbox=sandbox)
    assert result.output == 'done'
    assert seen == ['direct']


async def test_run_argument_sandbox_available_in_earliest_hooks():
    """A run-argument sandbox is available from `for_run` and `wrap_run` entry onwards."""
    log: list[str] = []

    @dataclass
    class Watcher(AbstractCapability[Any]):
        async def for_run(self, ctx: RunContext[Any]) -> AbstractCapability[Any]:
            log.append(f'for_run:{_describe(ctx.sandbox)}')
            return self

        async def wrap_run(self, ctx: RunContext[Any], *, handler: Any) -> AgentRunResult[Any]:
            log.append(f'wrap_enter:{_describe(ctx.sandbox)}')
            return await handler()

        async def after_run(self, ctx: RunContext[Any], *, result: AgentRunResult[Any]) -> AgentRunResult[Any]:
            log.append(f'after_run:{_describe(ctx.sandbox)}')
            return result

    seen: list[str] = []
    agent = make_probe_agent(seen, capabilities=[Watcher()])
    await agent.run('go', sandbox=FakeSandbox('direct'))
    # Caller-owned: stays live through after_run, unlike capability-contributed sandboxes.
    assert log == ['for_run:direct', 'wrap_enter:direct', 'after_run:direct']


async def test_capability_sandbox_lifecycle_and_availability_window():
    seen: list[str] = []
    prototype = SandboxCapability(name='cap')
    agent = make_probe_agent(seen, capabilities=[prototype])
    result = await agent.run('go')
    assert result.output == 'done'
    assert seen == ['cap']
    # The prototype never acquired anything: `for_run` isolation puts the handle on the per-run copy.
    assert prototype.acquired is None
    assert prototype.log == [
        # wrap_run enters before the hook fires, so it can't see the sandbox yet...
        'cap:wrap_enter:none',
        # ...then the winner acquires, and before_run (and tools) see it...
        'cap:get_sandbox',
        'cap:before_run:cap',
        # ...and the slot is cleared before the wrap chain unwinds, so after_run sees none.
        'cap:wrap_exit',
        'cap:after_run:none',
    ]


async def test_run_argument_suppresses_capability_hook():
    seen: list[str] = []
    cap = SandboxCapability(name='loser')
    agent = make_probe_agent(seen, capabilities=[cap])
    await agent.run('go', sandbox=FakeSandbox('direct'))
    assert seen == ['direct']
    assert not any('get_sandbox' in entry for entry in cap.log)


async def test_last_capability_in_chain_wins_and_losers_never_acquire():
    seen: list[str] = []
    first = SandboxCapability(name='first')
    last = SandboxCapability(name='last')
    agent = make_probe_agent(seen, capabilities=[first, last])
    await agent.run('go')
    assert seen == ['last']
    assert not any('get_sandbox' in entry for entry in first.log)


async def test_innermost_ordering_constraint_beats_registration_order():
    """Selection follows the resolved chain, not raw registration: `innermost` sorts last and wins."""

    @dataclass
    class InnermostSandboxCapability(SandboxCapability):
        def get_ordering(self) -> CapabilityOrdering | None:
            return CapabilityOrdering(position='innermost')

    seen: list[str] = []
    pinned = InnermostSandboxCapability(name='pinned-innermost')
    later = SandboxCapability(name='registered-later')
    agent = make_probe_agent(seen, capabilities=[pinned, later])
    await agent.run('go')
    assert seen == ['pinned-innermost']
    assert not any('get_sandbox' in entry for entry in later.log)


async def test_capability_returning_none_falls_back_to_earlier_capability():
    @dataclass
    class Abstaining(SandboxCapability):
        async def get_sandbox(self, ctx: RunContext[Any]) -> Sandbox | None:
            self.log.append(f'{self.name}:get_sandbox')
            return None

    seen: list[str] = []
    contributor = SandboxCapability(name='contributor')
    abstainer = Abstaining(name='abstainer')
    agent = make_probe_agent(seen, capabilities=[contributor, abstainer])
    await agent.run('go')
    # The later capability was consulted first, returned None, and the earlier one won.
    assert seen == ['contributor']
    assert any('abstainer:get_sandbox' in entry for entry in abstainer.log)


async def test_deferred_unloaded_capability_never_contributes():
    seen: list[str] = []
    deferred = SandboxCapability(name='deferred')
    deferred.id = 'deferred-sandbox'
    deferred.defer_loading = True
    agent = make_probe_agent(seen, capabilities=[deferred])
    await agent.run('go')
    # Not loaded: its wrap_run never entered, so acquiring here would leak — it must be skipped.
    assert seen == ['none']
    assert not any('get_sandbox' in entry for entry in deferred.log)


async def test_sandbox_identity_stable_across_steps():
    """Two tool calls in different run steps observe the same sandbox object."""
    observed: list[Any] = []

    def model_func(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart('probe', {})])
        elif len(messages) == 3:
            return ModelResponse(parts=[ToolCallPart('probe', {})])
        return ModelResponse(parts=[TextPart('done')])

    agent: Agent = Agent(FunctionModel(model_func), capabilities=[SandboxCapability(name='stable')])

    @agent.tool
    async def probe(ctx: RunContext[Any]) -> str:
        observed.append(ctx.sandbox)
        return 'ok'

    await agent.run('go')
    assert len(observed) == 2
    assert observed[0] is observed[1]


async def test_capability_sandbox_cleared_before_toolset_exit():
    """Capability teardown (wrap_run finally) runs before toolset __aexit__, so the toolset
    must see `None` — never a live-looking destroyed handle."""
    exit_observations: list[str] = []

    class Observing(WrapperToolset[Any]):
        async def __aexit__(self, *args: Any) -> bool | None:
            from pydantic_ai._run_context import get_current_run_context

            ctx = get_current_run_context()
            exit_observations.append(_describe(ctx.sandbox) if ctx is not None else 'no-ctx')
            return await super().__aexit__(*args)

    toolset: FunctionToolset[Any] = FunctionToolset()

    @toolset.tool
    async def probe(ctx: RunContext[Any]) -> str:
        assert ctx.sandbox is not None
        return 'ok'

    agent: Agent = Agent(
        _tool_call_then_text(),
        toolsets=[Observing(wrapped=toolset)],
        capabilities=[SandboxCapability(name='cap')],
    )
    await agent.run('go')
    assert exit_observations in (['none'], ['no-ctx'])  # cleared (or no ambient ctx) — never 'cap'


async def test_on_run_error_sees_cleared_sandbox():
    cap = SandboxCapability(name='cap')

    def model_func(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ToolCallPart('explode', {})])

    agent: Agent = Agent(FunctionModel(model_func), capabilities=[cap])

    @agent.tool
    async def explode(ctx: RunContext[Any]) -> str:
        assert ctx.sandbox is not None
        raise RuntimeError('boom')

    with pytest.raises(RuntimeError, match='boom'):
        await agent.run('go')
    assert any(entry == 'cap:on_run_error:none' for entry in cap.log)


async def test_sandbox_alive_for_streamed_run_consumption():
    cap = SandboxCapability(name='cap')
    agent: Agent = Agent(TestModel(custom_output_text='hello world'), capabilities=[cap])

    async with agent.run_stream('go') as stream:
        acquired = cap.log.count('cap:get_sandbox')
        assert acquired == 1
        assert 'cap:wrap_exit' not in cap.log  # teardown must not have run mid-stream
        async for _chunk in stream.stream_text():
            pass
    assert 'cap:wrap_exit' in cap.log


async def test_wrapper_capability_forwards_get_sandbox():
    seen: list[str] = []
    inner = SandboxCapability(name='inner')
    wrapper = WrapperCapability(wrapped=inner)
    assert wrapper.has_get_sandbox
    agent = make_probe_agent(seen, capabilities=[wrapper])
    await agent.run('go')
    assert seen == ['inner']


def test_has_get_sandbox_detection():
    assert SandboxCapability().has_get_sandbox

    @dataclass
    class Plain(AbstractCapability[Any]):
        pass

    assert not Plain().has_get_sandbox
    assert not WrapperCapability(wrapped=Plain()).has_get_sandbox


async def test_wrap_run_error_before_handler_never_resolves_sandbox():
    """If a wrap_run fails before calling its handler, `get_sandbox` never fires."""
    cap = SandboxCapability(name='cap')

    @dataclass
    class Exploding(AbstractCapability[Any]):
        async def wrap_run(self, ctx: RunContext[Any], *, handler: Any) -> AgentRunResult[Any]:
            raise RuntimeError('pre-handler failure')

    seen: list[str] = []
    agent = make_probe_agent(seen, capabilities=[cap, Exploding()])
    with pytest.raises(RuntimeError, match='pre-handler failure'):
        await agent.run('go')
    assert not any('get_sandbox' in entry for entry in cap.log)
