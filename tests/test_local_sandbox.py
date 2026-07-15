"""Tests for the shipped minimal `LocalSandbox` implementation of the sandbox protocol."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest

from pydantic_ai import Agent, LocalSandbox, RunContext
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.sandbox import Sandbox

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(os.name != 'posix', reason='LocalSandbox tests drive POSIX shell commands'),
]


def test_non_posix_platforms_are_rejected_honestly(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(os, 'name', 'nt')
    with pytest.raises(NotImplementedError, match='only supports POSIX'):
        LocalSandbox()


def test_local_sandbox_conforms_to_the_protocol(tmp_path: Path):
    sandbox = LocalSandbox(tmp_path)
    assert isinstance(sandbox, Sandbox)
    typed: Sandbox = sandbox  # static conformance, checked because tests are type-checked
    assert typed.provider == 'local'
    assert sandbox.sandbox_id.startswith('local-')


async def test_run_argv_and_shell(tmp_path: Path):
    sandbox = LocalSandbox(tmp_path)
    result = await sandbox.run(['echo', 'hello'])
    assert (result.exit_code, result.stdout, result.stderr) == (0, 'hello\n', '')
    assert result.stdout_dropped == 0 and result.stderr_dropped == 0
    shell_result = await sandbox.run('echo foo | tr a-z A-Z', shell=True)
    assert shell_result.stdout == 'FOO\n'


async def test_shell_discipline(tmp_path: Path):
    sandbox = LocalSandbox(tmp_path)
    with pytest.raises(TypeError, match='requires shell=True'):
        await sandbox.run('echo hello')
    with pytest.raises(TypeError, match='single command string'):
        await sandbox.run(['echo', 'hello'], shell=True)


async def test_nonzero_exit_is_a_result(tmp_path: Path):
    sandbox = LocalSandbox(tmp_path)
    result = await sandbox.run('echo oops >&2; exit 3', shell=True)
    assert result.exit_code == 3
    assert result.stderr == 'oops\n'


async def test_timeout_kills_the_whole_process_group_and_raises(tmp_path: Path):
    sandbox = LocalSandbox(tmp_path)
    marker = tmp_path / 'survived'
    with pytest.raises(TimeoutError):
        # The GRANDCHILD subshell writes the marker: killing only the direct `sh`
        # child would leave it alive to write — only a group kill prevents it.
        await sandbox.run(f'(sleep 1 && echo alive > {marker}) & wait', shell=True, timeout=0.2)
    await asyncio.sleep(1.3)  # past the point where a surviving process would write
    assert not marker.exists()


async def test_env_overlays_the_host_environment(tmp_path: Path):
    sandbox = LocalSandbox(tmp_path)
    result = await sandbox.run('echo "$GREETING:$PATH"', shell=True, env={'GREETING': 'hi'})
    greeting, _, path = result.stdout.rstrip('\n').partition(':')
    assert greeting == 'hi'
    assert path  # PATH survived the overlay: env= augments, it does not replace

    cwd_result = await sandbox.run(['pwd'], cwd=str(tmp_path))
    assert cwd_result.stdout.rstrip('\n').endswith(tmp_path.name)


async def test_optional_operations_are_honestly_absent(tmp_path: Path):
    sandbox = LocalSandbox(tmp_path)
    with pytest.raises(NotImplementedError, match='bound it in-command'):
        await sandbox.run(['echo', 'hi'], output_limit=10)
    with pytest.raises(NotImplementedError, match='only supports run'):
        await sandbox.start(['echo', 'hi'])


async def test_working_dir_and_resolve(tmp_path: Path):
    sandbox = LocalSandbox(tmp_path)
    assert await sandbox.working_dir() == str(tmp_path)
    assert await sandbox.resolve('notes.txt') == f'{tmp_path}/notes.txt'
    assert await sandbox.resolve('sub/../notes.txt') == f'{tmp_path}/notes.txt'
    assert await sandbox.resolve('/abs/./x') == '/abs/x'
    assert await sandbox.resolve('x', base='/elsewhere') == '/elsewhere/x'


async def test_filesystem_round_trip_with_parent_creation(tmp_path: Path):
    sandbox = LocalSandbox(tmp_path)
    nested = await sandbox.resolve('a/b/notes.txt')
    await sandbox.fs.write_text(nested, 'hello')  # the write contract creates parents
    assert await sandbox.fs.read_text(nested) == 'hello'
    entry = await sandbox.fs.stat(nested)
    assert (entry.name, entry.is_dir, entry.size) == ('notes.txt', False, 5)

    payload = bytes(range(256))
    blob = await sandbox.resolve('blob.bin')
    await sandbox.fs.write_bytes(blob, payload)
    assert await sandbox.fs.read_bytes(blob) == payload

    directory = await sandbox.resolve('a')
    assert (await sandbox.fs.stat(directory)).is_dir
    names = [entry.name for entry in await sandbox.fs.list_dir(str(tmp_path))]
    assert names == ['a', 'blob.bin']

    made = await sandbox.resolve('made/deep')
    await sandbox.fs.make_dir(made)
    await sandbox.fs.make_dir(made)  # mkdir -p semantics
    assert await sandbox.fs.exists(made)

    await sandbox.fs.remove(directory)  # removes the tree
    assert not await sandbox.fs.exists(nested)
    await sandbox.fs.remove(blob)
    assert not await sandbox.fs.exists(blob)
    with pytest.raises(FileNotFoundError):
        await sandbox.fs.read_bytes(blob)


async def test_unused_default_sandbox_creates_no_directory():
    async with LocalSandbox() as sandbox:
        pass  # never used: the lazy default root was never created
    assert sandbox._root is None  # pyright: ignore[reportPrivateUsage]


async def test_default_root_is_a_temp_dir_removed_on_exit():
    async with LocalSandbox() as sandbox:
        root = Path(await sandbox.working_dir())
        assert root.exists()
        await sandbox.fs.write_text(str(root / 'x.txt'), 'x')
    assert not root.exists()


async def test_caller_supplied_root_is_never_removed(tmp_path: Path):
    async with LocalSandbox(tmp_path) as sandbox:
        await sandbox.fs.write_text(str(tmp_path / 'keep.txt'), 'kept')
    assert (tmp_path / 'keep.txt').read_text() == 'kept'


async def test_agent_run_end_to_end(tmp_path: Path):
    def model_func(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart('execute', {'command': 'echo $((6*7))'})])
        return ModelResponse(parts=[TextPart('done')])

    agent: Agent = Agent(FunctionModel(model_func))
    outputs: list[str] = []

    @agent.tool
    async def execute(ctx: RunContext[Any], command: str) -> str:
        sandbox = ctx.sandbox
        assert sandbox is not None
        result = await sandbox.run(command, shell=True, timeout=30)
        outputs.append(result.stdout)
        return result.stdout

    async with LocalSandbox(tmp_path) as sandbox:
        result = await agent.run('compute 6*7 in the sandbox', sandbox=sandbox)

    assert result.output == 'done'
    assert outputs == ['42\n']
