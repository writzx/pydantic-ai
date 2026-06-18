from __future__ import annotations as _annotations

from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from copy import copy
from dataclasses import dataclass, field, replace
from decimal import Decimal
from datetime import datetime
from functools import cached_property
from types import TracebackType
from typing import TYPE_CHECKING, Any, NoReturn, TypeGuard

import anyio
from opentelemetry.trace import get_current_span
from opentelemetry.util.types import AttributeValue
from typing_extensions import assert_never

from pydantic_ai._instrumentation import model_attributes, model_request_parameters_attributes
from pydantic_ai._run_context import RunContext
<<<<<<< HEAD
from pydantic_ai._utils import await_maybe, get_first_param_type, now_utc as _now_utc

from .._genai_prices import fill_response_cost
from ..exceptions import FallbackExceptionGroup, ModelAPIError, UserError
from ..messages import ModelResponse, ModelResponseResetEvent, ModelResponseStreamEvent
from ..profiles import ModelProfile
from . import (
    KnownModelName,
    Model,
    ModelRequestParameters,
    StreamedResponse,
    infer_model,
)

if TYPE_CHECKING:
    from ..messages import ModelMessage
    from ..settings import ModelSettings

_PYDANTIC_AI_METADATA_KEY = '__pydantic_ai__'
_FALLBACK_MODEL_ID_KEY = 'fallback_model_id'
# Must match `_continuation._REPLACE_PREVIOUS_RESPONSE_KEY`: the merge module reads this exact key
# (under `__pydantic_ai__`) to fold a post-rewind response as a replace. Duplicated as a literal rather
# than imported because that constant is module-private (importing it trips `reportPrivateUsage`).
_REPLACE_PREVIOUS_RESPONSE_KEY = 'replace_previous_response'

ExceptionHandler = Callable[[Exception], Awaitable[bool]] | Callable[[Exception], bool]
"""A sync or async callable that decides whether an exception should trigger fallback."""

ResponseHandler = Callable[[ModelResponse], Awaitable[bool]] | Callable[[ModelResponse], bool]
"""A sync or async callable that decides whether a model response should trigger fallback."""

FallbackOn = (
    type[Exception]
    | tuple[type[Exception], ...]
    | ExceptionHandler
    | ResponseHandler
    | Sequence[type[Exception] | ExceptionHandler | ResponseHandler]
)
"""The type of the `fallback_on` parameter to [`FallbackModel`][pydantic_ai.models.fallback.FallbackModel]."""


class ResponseRejected(Exception):
    """Raised within a `FallbackExceptionGroup` when model responses are rejected by a response handler."""

    def __init__(self, rejected_count: int):
        super().__init__(f'{rejected_count} model response(s) rejected by fallback_on handler')


def _is_response_handler(handler: Callable[..., Any]) -> bool:
    """Check if a callable is a response handler based on type hints.

    Returns True if the first parameter is type-hinted as ModelResponse.
    Returns False otherwise (including if there are no type hints).
    """
    first_param_type = get_first_param_type(handler)
    if first_param_type is None:
        return False
    # Only support exact ModelResponse type (no Optional, no subclasses)
    return first_param_type is ModelResponse


def _is_exception_type(value: Any) -> TypeGuard[type[Exception]]:
    """Check if value is a single exception type."""
    return isinstance(value, type) and issubclass(value, Exception)


@dataclass(frozen=True)
class _StreamRequestArgs:
    messages: list[ModelMessage]
    model_settings: ModelSettings | None
    model_request_parameters: ModelRequestParameters
    run_context: RunContext[Any] | None


@dataclass(kw_only=True)
class _FallbackStreamedResponse(StreamedResponse):
    _models: list[Model]
    _request_args: _StreamRequestArgs
    _exit_stack: AsyncExitStack
    _should_fallback_handler: Callable[[Exception | ModelResponse], Awaitable[bool]]
    _on_model_selected: Callable[[Model, ModelRequestParameters], None]
    _next_model_index: int = field(default=0, init=False)
    _exceptions: list[Exception] = field(default_factory=list[Exception], init=False)
    _rejected_responses: list[ModelResponse] = field(default_factory=list[ModelResponse], init=False)
    _current_model: Model | None = field(default=None, init=False)
    _current_stream: StreamedResponse | None = field(default=None, init=False)
    _current_prepared_parameters: ModelRequestParameters | None = field(default=None, init=False)
    _accepted_model: Model | None = field(default=None, init=False)
    _created_timestamp: datetime = field(default_factory=_now_utc, init=False)

    async def open_next_stream(self) -> None:
        while self._next_model_index < len(self._models):
            model = self._models[self._next_model_index]
            self._next_model_index += 1

            try:
                _, prepared_parameters = model.prepare_request(
                    self._request_args.model_settings, self._request_args.model_request_parameters
                )
                prepared_messages = model.prepare_messages(self._request_args.messages)
                stream = await self._exit_stack.enter_async_context(
                    model.request_stream(
                        prepared_messages,
                        self._request_args.model_settings,
                        self._request_args.model_request_parameters,
                        self._request_args.run_context,
                    )
                )
            except Exception as exc:
                if await self._should_fallback_handler(exc):
                    self._exceptions.append(exc)
                    continue
                raise exc

            self._current_model = model
            self._current_stream = stream
            self._current_prepared_parameters = prepared_parameters
            # Rebind to the new candidate's parts manager once per candidate;
            # the reference stays stable for the rest of this candidate's events.
            self._parts_manager = stream._parts_manager
            self._sync_current_stream()
            return

        _raise_fallback_exception_group(self._exceptions, self._rejected_responses)

    async def _get_event_iterator(self) -> AsyncIterator[ModelResponseStreamEvent]:
        while current_stream := self._current_stream:
            try:
                async for event in current_stream._get_event_iterator():
                    self._sync_current_stream()
                    yield event
            except Exception as exc:
                self._sync_current_stream()
                if await self._should_fallback_handler(exc):
                    self._exceptions.append(exc)
                    # Yield the reset BEFORE `open_next_stream()` swaps the parts manager so
                    # `iterator_with_part_end` (wrapping our events) closes the discarded part
                    # against the rejected candidate's parts manager. Reordering these two lines
                    # would cause the accepted candidate's content to leak into a part-end at the
                    # old part index.
                    yield ModelResponseResetEvent(discarded_response=current_stream.get())
                    await self.open_next_stream()
                    continue
                raise exc

            current_stream._finished = True
            self._sync_current_stream()
            response = current_stream.get()
            if await self._should_fallback_handler(response):
                self._rejected_responses.append(response)
                # See note above on ordering: emit the reset before swapping streams.
                yield ModelResponseResetEvent(discarded_response=response)
                await self.open_next_stream()
                continue

            assert self._current_model is not None
            assert self._current_prepared_parameters is not None
            self._accepted_model = self._current_model
            self._on_model_selected(self._current_model, self._current_prepared_parameters)
            return

    async def close_stream(self) -> None:
        if self._current_stream is None:
            return
        await self._current_stream.close_stream()

    def get_stream_cancel_errors(self) -> tuple[type[BaseException], ...]:
        if self._current_stream is None:
            return super().get_stream_cancel_errors()
        return self._current_stream.get_stream_cancel_errors()

    def _sync_current_stream(self) -> None:
        # Per-event mirror of the inner stream's mutable state. `_parts_manager` is rebound in
        # `open_next_stream` (it doesn't change within a candidate). Provider id/details can be
        # set lazily on the first chunk for some providers (e.g. OpenAI), so we keep mirroring
        # them — once set they stay stable for the rest of the candidate.
        if self._current_stream is None:
            return
        self._usage = self._current_stream.usage
        self.provider_response_id = self._current_stream.provider_response_id
        self.provider_details = self._current_stream.provider_details
        self.finish_reason = self._current_stream.finish_reason

    @property
    def model_name(self) -> str:
        if self._accepted_model is not None:
            return self._accepted_model.model_name
        # `open_next_stream()` either sets `_current_model` or raises before we yield, so
        # by the time anyone reads `model_name` we always have a current model.
        assert self._current_model is not None
        return self._current_model.model_name

    @property
    def provider_name(self) -> str | None:
        if self._current_stream is None:
            return None
        return self._current_stream.provider_name

    @property
    def provider_url(self) -> str | None:
        if self._current_stream is None:
            return None
        return self._current_stream.provider_url

    @property
    def timestamp(self) -> datetime:
        if self._current_stream is None:
            return self._created_timestamp
        return self._current_stream.timestamp


@dataclass(init=False)
class FallbackModel(Model):
    """A model that uses one or more fallback models upon failure.

    Apart from `__init__`, all methods are private or match those of the base class.
    """

    models: list[Model]

    _exception_handlers: list[ExceptionHandler] = field(repr=False)
    _response_handlers: list[ResponseHandler] = field(repr=False)

    @cached_property
    def _enter_lock(self) -> anyio.Lock:
        # We use a cached_property for this because `anyio.Lock` binds to the event loop on which
        # it's first used; deferring creation until first access ensures it binds to the correct
        # running loop and avoids issues with Temporal's workflow sandbox.
        return anyio.Lock()

    def __init__(
        self,
        default_model: Model | KnownModelName | str,
        *fallback_models: Model | KnownModelName | str,
        fallback_on: FallbackOn = (ModelAPIError,),
    ):
        """Initialize a fallback model instance.

        Args:
            default_model: The name or instance of the default model to use.
            fallback_models: The names or instances of the fallback models to use upon failure.
            fallback_on: Conditions that trigger fallback to the next model. Accepts:

                - A tuple of exception types: `(ModelAPIError, RateLimitError)`
                - An exception handler (sync or async): `lambda exc: isinstance(exc, MyError)`
                - A response handler (sync or async): `def check(r: ModelResponse) -> bool`
                - A sequence mixing all of the above: `[ModelAPIError, exc_handler, response_handler]`

                Handler type is auto-detected by inspecting type hints on the first parameter.
                If the first parameter is hinted as `ModelResponse`, it's a response handler.
                Otherwise (including untyped handlers and lambdas), it's an exception handler.
        """
        super().__init__()
        self.models = [infer_model(default_model), *[infer_model(m) for m in fallback_models]]
        self._entered_count = 0

        # Parse fallback_on into exception handlers and response handlers
        self._exception_handlers = []
        self._response_handlers = []
        self._parse_fallback_on(fallback_on)

    def _parse_fallback_on(self, fallback_on: FallbackOn) -> None:
        """Parse the fallback_on parameter into exception and response handlers."""
        if isinstance(fallback_on, tuple):
            if fallback_on:
                # Tuple of exception types (typing guarantees tuple contents are exception types)
                self._exception_handlers.append(_exception_types_to_handler(fallback_on))  # type: ignore[arg-type]
        elif _is_exception_type(fallback_on):
            # Single exception type
            self._exception_handlers.append(_exception_types_to_handler((fallback_on,)))
        elif callable(fallback_on):
            # Single callable - auto-detect by type hints
            self._add_handler(fallback_on)
        elif isinstance(fallback_on, Sequence) and not isinstance(fallback_on, (str, bytes)):
            # Sequence of mixed handlers/types
            for item in fallback_on:
                if _is_exception_type(item):
                    self._exception_handlers.append(_exception_types_to_handler((item,)))
                elif callable(item):
                    self._add_handler(item)
                else:
                    # Types guarantee all items are exception types or callables
                    assert_never(item)
        else:
            assert_never(fallback_on)  # type: ignore[arg-type]  # pyright can't narrow str/bytes exclusion

        if not self._exception_handlers and not self._response_handlers:
            raise UserError(
                'FallbackModel created with empty fallback_on. '
                'All exceptions will propagate and all responses will be accepted. '
                'Use fallback_on=(ModelAPIError,) for default behavior.'
            )

    def _add_handler(self, handler: Callable[..., Any]) -> None:
        """Add a handler, auto-detecting its type by inspecting type hints."""
        if _is_response_handler(handler):
            self._response_handlers.append(handler)
        else:
            self._exception_handlers.append(handler)

    async def _should_fallback(self, value: Exception | ModelResponse) -> bool:
        """Check if any handler wants to trigger fallback."""
        handlers = self._exception_handlers if isinstance(value, Exception) else self._response_handlers
        for handler in handlers:
            # pyright can't narrow handler's param type from the isinstance check on value
            result = await await_maybe(handler(value))  # type: ignore[arg-type]
            if result:
                return True
        return False

    async def __aenter__(self) -> FallbackModel:
        """Enter all sub-models so their providers can manage HTTP client lifecycle."""
        async with self._enter_lock:
            if self._entered_count == 0:
                async with AsyncExitStack() as exit_stack:
                    for model in self.models:
                        await exit_stack.enter_async_context(model)
                    self._exit_stack = exit_stack.pop_all()
            self._entered_count += 1
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Exit all sub-models, closing their providers' HTTP clients."""
        async with self._enter_lock:
            self._entered_count -= 1
            if self._entered_count == 0:
                await self._exit_stack.aclose()

    @property
    def provider(self) -> None:
        return None  # pragma: no cover

    @property
    def model_name(self) -> str:
        """The model name."""
        return f'fallback:{",".join(model.model_name for model in self.models)}'

    @property
    def model_id(self) -> str:
        """The fully qualified model identifier, combining the wrapped models' IDs."""
        return f'fallback:{",".join(model.model_id for model in self.models)}'

    @property
    def system(self) -> str:
        return f'fallback:{",".join(model.system for model in self.models)}'

    @property
    def base_url(self) -> str | None:
        return self.models[0].base_url

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        """Try each model in sequence until one succeeds.

        In case of failure, raise a FallbackExceptionGroup with all exceptions.

        If a previous response set `state='suspended'`, the request is routed directly
        to the pinned continuation model, bypassing the fallback chain. If the pinned model
        raises a fallback-eligible error during continuation, the messages are rewound
        (stripping the suspended response and trailing continuation request) and the
        normal fallback chain is tried.
        """
        exceptions: list[Exception] = []
        rejected_responses: list[ModelResponse] = []
        rejected_cost: Decimal | None = None
        # Set once a pinned continuation fails and we rewind to the chain: the first successful response
        # the chain then produces is fresh generation superseding the stale suspended turn, so it must
        # be stamped as a replace (see `_stamp_replace_previous`) rather than accumulated onto it.
        rewound = False

        if pinned := self._get_continuation_model(messages):
            # `_get_continuation_model` only returns a model when the last message is a suspended response.
            suspended_response = messages[-1]
            assert isinstance(suspended_response, ModelResponse)
            prepared_parameters = model_request_parameters
            try:
                _, prepared_parameters = pinned.prepare_request(model_settings, model_request_parameters)
                prepared_messages = pinned.prepare_messages(messages, model_request_parameters)
                response = await pinned.request(prepared_messages, model_settings, model_request_parameters)
            except Exception as exc:
                if not await self._should_fallback(exc):
                    self._set_span_attributes(pinned, prepared_parameters)
                    raise
                # Best-effort cancel the suspended server-side job we're abandoning before rewinding
                # and retrying the chain. `FallbackModel` swallows the error, so the graph's own
                # cancel path never sees it; without this an OpenAI background job would keep running
                # and billing while the chain issues a duplicate request.
                with suppress(Exception):
                    await pinned.cancel_suspended_response(suspended_response)
                messages = _rewind_messages(messages)
                rewound = True
                exceptions.append(exc)
                # Fall through to normal chain below
            else:
                if response.state == 'suspended':
                    _stamp_continuation(response, pinned)
                self._set_span_attributes(pinned, prepared_parameters)
                return response

        for model in self.models:
            prepared_parameters = model_request_parameters
            try:
                _, prepared_parameters = model.prepare_request(model_settings, model_request_parameters)
                # Each inner model has its own profile, so re-run `prepare_messages` per model.
                prepared_messages = model.prepare_messages(messages, model_request_parameters)
                response = await model.request(prepared_messages, model_settings, model_request_parameters)
            except Exception as exc:
                if await self._should_fallback(exc):
                    exceptions.append(exc)
                    continue
                self._set_span_attributes(model, prepared_parameters)
                raise exc

            if await self._should_fallback(response):
                fill_response_cost(response)
                if response.usage.cost is not None:
                    rejected_cost = (rejected_cost or Decimal()) + response.usage.cost
                rejected_responses.append(response)
                continue

            if rejected_cost is not None:
                fill_response_cost(response)
                usage = copy(response.usage)
                usage.cost = (usage.cost or Decimal()) + rejected_cost
                response = replace(response, usage=usage)

            # After a rewind, the first successful response is fresh generation that supersedes the
            # abandoned suspended turn (whether it ends complete or suspended), so mark it as a replace.
            if rewound:
                _stamp_replace_previous(response)
            if response.state == 'suspended':
                _stamp_continuation(response, model)
            self._set_span_attributes(model, prepared_parameters)
            return response

        _raise_fallback_exception_group(exceptions, rejected_responses)

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncGenerator[StreamedResponse]:
        """Try each model in sequence until one succeeds.

        A response handler uses a composite stream so rejected responses and mid-stream failures can
        reset the consumer and continue with the next fallback model.

        If a previous response set `state='suspended'`, the request is routed directly
        to the pinned continuation model, bypassing the fallback chain. If the pinned model
        raises a fallback-eligible error while opening the stream, the messages are rewound
        and the normal fallback chain is tried. Mid-stream failures still propagate.
        """
        if self._response_handlers:
            async with AsyncExitStack() as stack:
                response = _FallbackStreamedResponse(
                    model_request_parameters=model_request_parameters,
                    _models=self.models,
                    _request_args=_StreamRequestArgs(
                        messages=messages,
                        model_settings=model_settings,
                        model_request_parameters=model_request_parameters,
                        run_context=run_context,
                    ),
                    _exit_stack=stack,
                    _should_fallback_handler=self._should_fallback,
                    _on_model_selected=self._set_span_attributes,
                )
                await response.open_next_stream()
                yield response
                return
        exceptions: list[Exception] = []
        # Set once a pinned continuation fails and we rewind to the chain: see the non-streaming `request`.
        rewound = False

        if pinned := self._get_continuation_model(messages):
            # `_get_continuation_model` only returns a model when the last message is a suspended response.
            suspended_response = messages[-1]
            assert isinstance(suspended_response, ModelResponse)
            async with AsyncExitStack() as stack:
                prepared_parameters = model_request_parameters
                try:
                    _, prepared_parameters = pinned.prepare_request(model_settings, model_request_parameters)
                    prepared_messages = pinned.prepare_messages(messages, model_request_parameters)
                    streamed_response = await stack.enter_async_context(
                        pinned.request_stream(prepared_messages, model_settings, model_request_parameters, run_context)
                    )
                except Exception as exc:
                    if not await self._should_fallback(exc):
                        self._set_span_attributes(pinned, prepared_parameters)
                        raise
                    # Best-effort cancel the suspended server-side job we're abandoning before
                    # rewinding to the chain (see the non-streaming path above); `FallbackModel`
                    # swallows the error, so the graph's own cancel path never sees it.
                    with suppress(Exception):
                        await pinned.cancel_suspended_response(suspended_response)
                    messages = _rewind_messages(messages)
                    rewound = True
                    exceptions.append(exc)
                    # Fall through to normal chain below
                else:
                    self._set_span_attributes(pinned, prepared_parameters)
                    yield streamed_response
                    # Unlike `request()`, which stamps before returning, the streaming path stamps
                    # after `yield`: the final `state` is only known once the caller has consumed the
                    # stream. Callers must therefore call `get()` after the `async with` exits.
                    if streamed_response.state == 'suspended':
                        _stamp_continuation(streamed_response, pinned)
                    return

        for model in self.models:
            async with AsyncExitStack() as stack:
                prepared_parameters = model_request_parameters
                try:
                    _, prepared_parameters = model.prepare_request(model_settings, model_request_parameters)
                    prepared_messages = model.prepare_messages(messages, model_request_parameters)
                    streamed_response = await stack.enter_async_context(
                        model.request_stream(prepared_messages, model_settings, model_request_parameters, run_context)
                    )
                except Exception as exc:
                    if await self._should_fallback(exc):
                        exceptions.append(exc)
                        continue
                    self._set_span_attributes(model, prepared_parameters)
                    raise exc

                # After a rewind, mark this fresh stream as replacing the abandoned suspended turn.
                # Unlike the continuation pin (stamped after `yield`, once the final `state` is known),
                # this must land on `metadata` *before* `yield`: the streamed composite resolves
                # `_segment_offset` (via `merge_mode`) on the first reindexable event, so a late stamp
                # would reindex against a stale `'accumulate'` verdict and misplace the parts. That this
                # stream supersedes the suspended turn is known the moment the rewound chain is entered.
                if rewound:
                    _stamp_replace_previous(streamed_response)
                self._set_span_attributes(model, prepared_parameters)
                yield streamed_response
                # Stamp after `yield` (see the pinned path above): `state` is only final once the
                # caller has consumed the stream, so callers must call `get()` after the context exits.
                if streamed_response.state == 'suspended':
                    _stamp_continuation(streamed_response, model)
                return

        _raise_fallback_exception_group(exceptions, [])

    async def cancel_suspended_response(self, response: ModelResponse) -> None:
        """Cancel a suspended continuation on the underlying model holding the server-side job.

        When the response carries a continuation pin, resolve that model and delegate to it. Resolve
        the pin directly from metadata rather than via `_get_continuation_model`: the cancel path is
        driven by `_ContinuationStreamedResponse.get()`, whose `state` is already
        `'interrupted'`/`'incomplete'`/`'complete'` (never `'suspended'`) by the time cancellation
        unwinds, so gating on `state == 'suspended'` here would never find the pin.

        When no pin resolves, the response can still hold a live server-side job: the pin is only
        stamped when a segment *ends* suspended, so a streamed background job cancelled during its
        first segment (e.g. OpenAI background mode, marked by `provider_details['background']` +
        `provider_response_id`) has no pin yet. Best-effort delegate to every inner model so the job
        is torn down rather than leaked. This is safe because each model's own cancel guard is strict
        (OpenAI only acts on its own `background` marker with a matching `provider_name`; others
        no-op), and a raising model doesn't stop the rest.
        """
        if pinned := self._pinned_continuation_model(response):
            await pinned.cancel_suspended_response(response)
            return

        for model in self.models:
            with suppress(Exception):
                await model.cancel_suspended_response(response)

    def continuation_delay(self, response: ModelResponse) -> float | None:
        if pinned := self._pinned_continuation_model(response):
            return pinned.continuation_delay(response)
        for model in self.models:
            if (delay := model.continuation_delay(response)) is not None:
                return delay
        return None

    @cached_property
    def profile(self) -> ModelProfile:
        raise NotImplementedError('FallbackModel does not have its own model profile.')

    @property
    def context_window(self) -> int | None:
        """The smallest known context window among the candidate models, or `None` if none is known.

        Any candidate may end up answering, and history that fits the smallest window fits them all,
        so compacting against it errs towards compacting early rather than overflowing a fallback.
        Candidates with an unknown window don't constrain the result.
        """
        windows = [window for model in self.models if (window := model.context_window) is not None]
        return min(windows) if windows else None

    def customize_request_parameters(self, model_request_parameters: ModelRequestParameters) -> ModelRequestParameters:
        return model_request_parameters  # pragma: no cover

    def prepare_request(
        self, model_settings: ModelSettings | None, model_request_parameters: ModelRequestParameters
    ) -> tuple[ModelSettings | None, ModelRequestParameters]:
        return model_settings, model_request_parameters

    def prepare_messages(
        self,
        messages: list[ModelMessage],
        model_request_parameters: ModelRequestParameters | None = None,
    ) -> list[ModelMessage]:
        # `FallbackModel` doesn't have its own profile; dispatch applies each inner model's profile instead.
        return messages

    def _get_continuation_model(self, messages: list[ModelMessage]) -> Model | None:
        """Find the model that should handle continuation from message history."""
        if not messages:  # pragma: lax no cover
            return None
        last = messages[-1]
        if not isinstance(last, ModelResponse) or last.state != 'suspended':
            return None
        return self._pinned_continuation_model(last)

    def _pinned_continuation_model(self, response: ModelResponse) -> Model | None:
        """Resolve the underlying model pinned to this continuation from its routing metadata."""
        pydantic_ai_meta = (response.metadata or {}).get(_PYDANTIC_AI_METADATA_KEY, {})
        if model_id := pydantic_ai_meta.get(_FALLBACK_MODEL_ID_KEY):
            return next((m for m in self.models if m.model_id == model_id), None)
        return None

    def _set_span_attributes(self, model: Model, model_request_parameters: ModelRequestParameters) -> None:
        with suppress(Exception):
            span = get_current_span()
            if span.is_recording():
                attributes = getattr(span, 'attributes', {})
                if attributes.get('gen_ai.request.model') == self.model_name:  # pragma: no branch
                    span_attributes: dict[str, AttributeValue] = {**model_attributes(model)}
                    # Only refresh `model_request_parameters` if it was emitted at span open; its absence
                    # means `InstrumentationSettings.include_model_request_parameters` is off, and re-adding
                    # it here would leak the attribute the setting is meant to suppress.
                    if 'model_request_parameters' in attributes:
                        span_attributes.update(model_request_parameters_attributes(model_request_parameters))
                    span.set_attributes(span_attributes)


def _stamp_continuation(response: ModelResponse | StreamedResponse, model: Model) -> None:
    """Stamp the model's identifier into metadata for stateless continuation routing.

    Uses `metadata['__pydantic_ai__']` to avoid conflating framework-level routing state
    with provider-specific data in `provider_details`.
    """
    if response.metadata is None:
        response.metadata = {}
    pydantic_ai_meta = response.metadata.setdefault(_PYDANTIC_AI_METADATA_KEY, {})
    pydantic_ai_meta[_FALLBACK_MODEL_ID_KEY] = model.model_id


def _stamp_replace_previous(response: ModelResponse | StreamedResponse) -> None:
    """Stamp the `replace_previous_response` marker so a fresh post-rewind turn supersedes the stale one.

    After a pinned continuation fails and `FallbackModel` rewinds and retries the chain, the first
    successful response is genuinely fresh generation, but may carry the same `model_name` as the
    abandoned suspended turn (only the `provider_response_id` differs). Without this marker
    `merge_mode` would classify the merge as an `accumulate` — same model, different id, indistinguishable
    from an Anthropic `pause_turn` — and duplicate the abandoned suspended parts ahead of the fresh turn.
    The marker (merged into the shared `__pydantic_ai__` namespace, alongside any continuation pin) tells
    the merge to `'replace-new'`; it's transient and popped after being honored so it can't persist into
    history. See `pydantic_ai.models._continuation`.
    """
    if response.metadata is None:
        response.metadata = {}
    pydantic_ai_meta = response.metadata.setdefault(_PYDANTIC_AI_METADATA_KEY, {})
    pydantic_ai_meta[_REPLACE_PREVIOUS_RESPONSE_KEY] = True


def _rewind_messages(messages: list[ModelMessage]) -> list[ModelMessage]:
    """Strip the suspended response from the end of message history.

    When a pinned continuation model fails, the messages still contain the suspended
    response. Before falling through to the normal chain, we remove it so models see
    clean history ending with the most recent ModelRequest.
    """
    rewound = list(messages)
    if rewound and isinstance(rewound[-1], ModelResponse) and rewound[-1].state == 'suspended':  # pragma: no branch
        rewound.pop()
    return rewound


def _exception_types_to_handler(exceptions: tuple[type[Exception], ...]) -> ExceptionHandler:
    """Create an exception handler from a tuple of exception types."""

    def handler(exc: Exception) -> bool:
        return isinstance(exc, exceptions)

    return handler


def _raise_fallback_exception_group(exceptions: list[Exception], rejected_responses: list[ModelResponse]) -> NoReturn:
    """Raise a FallbackExceptionGroup combining exceptions and response rejections.

    Args:
        exceptions: List of exceptions raised by models.
        rejected_responses: List of responses that were rejected by fallback_on handlers.
    """
    all_errors = list(exceptions)
    if rejected_responses:
        all_errors.append(ResponseRejected(len(rejected_responses)))
    raise FallbackExceptionGroup('All models from FallbackModel failed', all_errors)
