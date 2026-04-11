"""Caster -- connects to Rune runtime and executes registered handlers."""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import threading
import time
from typing import Callable, Awaitable

import grpc

from .pilot_client import PilotClient
from .types import FileAttachment, LoadReport, RuneConfig, RuneContext, ScalePolicy
from .stream import StreamSender
from .handler import RegisteredRune, OnceHandler

# Ensure _proto dir is on sys.path before importing generated code.
from . import _proto  # noqa: F401
from ._proto.rune.wire.v1 import rune_pb2, rune_pb2_grpc

logger = logging.getLogger("rune_framework")


class AttachRejectedError(RuntimeError):
    """Raised when the runtime rejects CasterAttach — a permanent error
    that must not be retried."""


class Caster:
    """Connects to a Rune runtime server and executes registered handlers."""

    def __init__(
        self,
        addr: str = "localhost:50070",
        caster_id: str = "python-caster",
        max_concurrent: int = 10,
        reconnect_base_delay: float = 1.0,
        reconnect_max_delay: float = 30.0,
        heartbeat_interval: float = 10.0,
        labels: dict[str, str] | None = None,
        api_key: str | None = None,
        scale_policy: ScalePolicy | None = None,
        load_report: LoadReport | None = None,
    ) -> None:
        self._addr = addr
        self._caster_id = caster_id
        self._max_concurrent = max_concurrent
        self._runes: dict[str, RegisteredRune] = {}
        self._cancelled: set[str] = set()
        self._reconnect_base_delay = reconnect_base_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._heartbeat_interval = heartbeat_interval
        self._labels = labels or {}
        self._api_key = api_key
        self._scale_policy = scale_policy
        self._load_report = load_report
        self._shutdown = threading.Event()
        self._active_requests = 0
        self._draining = False

    # ------------------------------------------------------------------
    # Decorator API
    # ------------------------------------------------------------------

    def rune(
        self,
        name: str,
        *,
        version: str = "0.0.0",
        description: str = "",
        gate: str | None = None,
        gate_method: str = "POST",
        input_schema: dict | None = None,
        output_schema: dict | None = None,
        priority: int = 0,
    ) -> Callable:
        """Decorator to register a unary rune handler."""

        def decorator(fn: OnceHandler) -> OnceHandler:
            config = RuneConfig(
                name=name,
                version=version,
                description=description,
                supports_stream=False,
                gate=gate,
                gate_method=gate_method,
                input_schema=input_schema,
                output_schema=output_schema,
                priority=priority,
            )
            accepts_files = _handler_accepts_files(fn)
            self._runes[name] = RegisteredRune(
                config=config, handler=fn, is_stream=False, accepts_files=accepts_files,
            )
            return fn

        return decorator

    def stream_rune(
        self,
        name: str,
        *,
        version: str = "0.0.0",
        description: str = "",
        gate: str | None = None,
        gate_method: str = "POST",
        input_schema: dict | None = None,
        output_schema: dict | None = None,
        priority: int = 0,
    ) -> Callable:
        """Decorator to register a streaming rune handler."""

        def decorator(fn: Callable) -> Callable:
            config = RuneConfig(
                name=name,
                version=version,
                description=description,
                supports_stream=True,
                gate=gate,
                gate_method=gate_method,
                input_schema=input_schema,
                output_schema=output_schema,
                priority=priority,
            )
            accepts_files = _handler_accepts_files(fn)
            self._runes[name] = RegisteredRune(
                config=config, handler=fn, is_stream=True, accepts_files=accepts_files,
            )
            return fn

        return decorator

    def scaler(self, *, policy: ScalePolicy) -> Callable:
        """Decorator to attach a scaling policy to the caster."""

        def decorator(fn: Callable) -> Callable:
            self._scale_policy = policy
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the caster (blocking). Runs asyncio event loop with auto-reconnect."""
        asyncio.run(self._run_with_reconnect())

    def stop(self) -> None:
        """Signal the caster to stop its run loop.

        Safe to call from any thread.  The ``run()`` method will return
        shortly after this is called.
        """
        self._shutdown.set()

    async def _run_with_reconnect(self) -> None:
        """Reconnect loop with exponential backoff."""
        delay = self._reconnect_base_delay
        last_pilot: PilotClient | None = None

        try:
            while not self._shutdown.is_set():
                # (Re-)establish pilot registration on every connect attempt
                # so that a pilot daemon restart is picked up automatically.
                pilot_id = None
                if self._scale_policy is not None:
                    try:
                        pilot = await PilotClient.ensure(self._addr, self._api_key)
                        await pilot.register(self._caster_id, self._scale_policy)
                        pilot_id = pilot.pilot_id
                        last_pilot = pilot
                    except Exception as e:
                        # Pilot failure is non-fatal — caster can still serve traffic.
                        logger.warning("pilot registration failed: %s", e)

                try:
                    await self._session(pilot_id)
                    # Session ended normally (detach) -- don't reconnect
                    logger.info("session ended normally")
                    break
                except grpc.aio.AioRpcError as e:
                    if self._shutdown.is_set():
                        break
                    logger.warning("gRPC error: %s, reconnecting in %.1fs", e.code(), delay)
                except AttachRejectedError:
                    # Permanent error — retrying won't help (e.g. invalid key).
                    raise
                except Exception as e:
                    if self._shutdown.is_set():
                        break
                    logger.warning("session error: %s, reconnecting in %.1fs", e, delay)

                # Wait for delay or shutdown, whichever comes first
                if self._shutdown.wait(timeout=delay):
                    break  # shutdown requested
                delay = min(delay * 2, self._reconnect_max_delay)
                logger.info("reconnecting to %s ...", self._addr)
        finally:
            if last_pilot is not None:
                try:
                    await last_pilot.deregister(self._caster_id)
                except Exception:
                    logger.debug("pilot deregister failed", exc_info=True)

    # ------------------------------------------------------------------
    # Attach message builder
    # ------------------------------------------------------------------

    def _build_attach_message(self, pilot_id: str | None = None) -> rune_pb2.SessionMessage:
        """Build the CasterAttach session message."""
        declarations = []
        for registered in self._runes.values():
            decl = rune_pb2.RuneDeclaration(
                name=registered.config.name,
                version=registered.config.version,
                description=registered.config.description,
                supports_stream=registered.config.supports_stream,
                input_schema=json.dumps(registered.config.input_schema) if registered.config.input_schema else "",
                output_schema=json.dumps(registered.config.output_schema) if registered.config.output_schema else "",
                priority=registered.config.priority,
            )
            if registered.config.gate:
                decl.gate.CopyFrom(
                    rune_pb2.GateConfig(
                        path=registered.config.gate,
                        method=registered.config.gate_method,
                    )
                )
            declarations.append(decl)

        return rune_pb2.SessionMessage(
            attach=rune_pb2.CasterAttach(
                caster_id=self._caster_id,
                runes=declarations,
                labels=self._attach_labels(pilot_id),
                max_concurrent=self._max_concurrent,
                key=self._api_key or "",
                role="caster",
            )
        )

    def _attach_labels(self, pilot_id: str | None = None) -> dict[str, str]:
        labels = dict(self._labels)
        if self._scale_policy is not None:
            labels["group"] = self._scale_policy.group
            labels["_scale_up"] = str(self._scale_policy.scale_up_threshold)
            labels["_scale_down"] = str(self._scale_policy.scale_down_threshold)
            labels["_sustained"] = str(self._scale_policy.sustained_secs)
            labels["_min"] = str(self._scale_policy.min_replicas)
            labels["_max"] = str(self._scale_policy.max_replicas)
            labels["_spawn_command"] = self._scale_policy.spawn_command
            labels["_shutdown_signal"] = self._scale_policy.shutdown_signal
            if pilot_id:
                labels["_pilot_id"] = pilot_id
        return labels

    def _build_health_report_message(self) -> rune_pb2.SessionMessage:
        metrics = dict(self._load_report.metrics) if self._load_report is not None else {}
        metrics.setdefault("active_requests", float(self._active_requests))
        metrics.setdefault("max_concurrent", float(self._max_concurrent))
        metrics.setdefault(
            "available_permits",
            float(max(0, self._max_concurrent - self._active_requests)),
        )
        computed_pressure = (
            0.0 if self._max_concurrent == 0 else self._active_requests / self._max_concurrent
        )
        pressure = (
            self._load_report.pressure
            if self._load_report is not None and self._load_report.pressure is not None
            else computed_pressure
        )
        status = (
            rune_pb2.HEALTH_STATUS_UNHEALTHY
            if self._draining
            else rune_pb2.HEALTH_STATUS_HEALTHY
        )
        return rune_pb2.SessionMessage(
            health_report=rune_pb2.HealthReport(
                status=status,
                active_requests=self._active_requests,
                error_rate=0.0,
                custom_info="",
                timestamp_ms=int(time.time() * 1000),
                error_rate_window_secs=0,
                pressure=pressure,
                metrics=metrics,
            )
        )

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------

    async def _session(self, pilot_id: str | None = None) -> None:
        """Run one gRPC session."""
        # Reset draining state from any previous shutdown cycle.
        self._draining = False
        async with grpc.aio.insecure_channel(self._addr) as channel:
            stub = rune_pb2_grpc.RuneServiceStub(channel)

            # Bidirectional stream
            outbound_queue: asyncio.Queue[rune_pb2.SessionMessage | None] = asyncio.Queue()

            async def outbound_iter():
                while True:
                    msg = await outbound_queue.get()
                    if msg is None:
                        break
                    yield msg

            call = stub.Session(outbound_iter())

            # Build and send attach message
            attach_msg = self._build_attach_message(pilot_id)
            await outbound_queue.put(attach_msg)

            # Start heartbeat task
            hb_task = asyncio.create_task(self._heartbeat_loop(outbound_queue))

            # Monitor shutdown event to cancel the gRPC call
            async def _watch_shutdown():
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._shutdown.wait)
                call.cancel()

            shutdown_task = asyncio.create_task(_watch_shutdown())

            try:
                async for msg in call:
                    if self._shutdown.is_set():
                        break

                    payload = msg.WhichOneof("payload")

                    if payload == "attach_ack":
                        if msg.attach_ack.accepted:
                            logger.info(
                                "attached to %s, caster_id=%s",
                                self._addr,
                                self._caster_id,
                            )
                            # Always send initial HealthReport regardless of scale_policy
                            await outbound_queue.put(self._build_health_report_message())
                        else:
                            raise AttachRejectedError(
                                f"attach rejected: {msg.attach_ack.reason}"
                            ) from None

                    elif payload == "execute":
                        req = msg.execute
                        # Increment BEFORE spawning so the drain loop always
                        # sees accepted-but-not-yet-started requests.
                        self._active_requests += 1
                        asyncio.create_task(self._handle_execute(req, outbound_queue))

                    elif payload == "cancel":
                        self._cancelled.add(msg.cancel.request_id)
                        logger.info("cancel requested: %s", msg.cancel.request_id)

                    elif payload == "heartbeat":
                        pass  # server heartbeat received

                    elif payload == "shutdown":
                        grace_ms = msg.shutdown.grace_period_ms
                        logger.info(
                            "shutdown requested: %s, grace_period_ms=%d",
                            msg.shutdown.reason,
                            grace_ms,
                        )
                        # Mark as draining to reject new Execute requests.
                        self._draining = True
                        # Immediately advertise UNHEALTHY so the runtime stops
                        # routing new work to this caster during the grace window.
                        await outbound_queue.put(self._build_health_report_message())
                        # Graceful drain: wait for in-flight requests to complete
                        # or until grace_period_ms expires, whichever comes first.
                        loop = asyncio.get_running_loop()
                        deadline = loop.time() + grace_ms / 1000.0
                        while self._active_requests > 0:
                            remaining = deadline - loop.time()
                            if remaining <= 0:
                                logger.warning(
                                    "grace period expired with %d active requests remaining",
                                    self._active_requests,
                                )
                                break
                            await asyncio.sleep(min(0.05, remaining))
                        self.stop()
                        break

            finally:
                shutdown_task.cancel()
                hb_task.cancel()
                await outbound_queue.put(None)  # stop outbound iter

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self, queue: asyncio.Queue) -> None:
        """Send periodic heartbeat."""
        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval)
                msg = rune_pb2.SessionMessage(
                    heartbeat=rune_pb2.Heartbeat(
                        timestamp_ms=int(time.time() * 1000),
                    )
                )
                await queue.put(msg)
                # Always send HealthReport regardless of scale_policy
                await queue.put(self._build_health_report_message())
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Execute dispatch
    # ------------------------------------------------------------------

    async def _handle_execute(
        self,
        req: rune_pb2.ExecuteRequest,
        outbound_queue: asyncio.Queue,
    ) -> None:
        """Execute a rune handler (once or stream).

        The caller increments ``_active_requests`` *before* spawning this task
        so that the drain loop never observes a false zero.  This method is
        responsible for decrementing in *all* exit paths via the outer
        ``finally`` block.
        """
        try:
            # Reject new requests while draining for graceful shutdown.
            if self._draining:
                await outbound_queue.put(
                    rune_pb2.SessionMessage(
                        result=rune_pb2.ExecuteResult(
                            request_id=req.request_id,
                            status=rune_pb2.STATUS_FAILED,
                            error=rune_pb2.ErrorDetail(
                                code="SHUTTING_DOWN",
                                message="caster is draining, no new requests accepted",
                            ),
                        )
                    )
                )
                return
            registered = self._runes.get(req.rune_name)
            if registered is None:
                await outbound_queue.put(
                    rune_pb2.SessionMessage(
                        result=rune_pb2.ExecuteResult(
                            request_id=req.request_id,
                            status=rune_pb2.STATUS_FAILED,
                            error=rune_pb2.ErrorDetail(
                                code="NOT_FOUND",
                                message=f"rune '{req.rune_name}' not found",
                            ),
                        )
                    )
                )
                return

            ctx = RuneContext(
                rune_name=req.rune_name,
                request_id=req.request_id,
                context=dict(req.context),
            )

            try:
                if registered.is_stream:
                    await self._execute_stream(registered, ctx, req, outbound_queue)
                else:
                    await self._execute_once(registered, ctx, req, outbound_queue)
            except Exception as e:
                logger.error("handler error for %s: %s", req.rune_name, e)
                await outbound_queue.put(
                    rune_pb2.SessionMessage(
                        result=rune_pb2.ExecuteResult(
                            request_id=req.request_id,
                            status=rune_pb2.STATUS_FAILED,
                            error=rune_pb2.ErrorDetail(
                                code="EXECUTION_FAILED",
                                message=str(e),
                            ),
                        )
                    )
                )
        finally:
            # NF-8: Always clean up the cancelled set to prevent unbounded growth
            self._cancelled.discard(req.request_id)
            self._active_requests -= 1

    async def _execute_once(
        self,
        registered: RegisteredRune,
        ctx: RuneContext,
        req: rune_pb2.ExecuteRequest,
        outbound_queue: asyncio.Queue,
    ) -> None:
        """Execute unary handler."""
        if registered.accepts_files:
            attachments = _extract_attachments(req)
            result = registered.handler(ctx, bytes(req.input), attachments)
        else:
            result = registered.handler(ctx, bytes(req.input))
        output = await _ensure_awaitable(result)

        # Check if cancelled during execution
        if req.request_id in self._cancelled:
            self._cancelled.discard(req.request_id)
            logger.info("request %s cancelled during execution, discarding", req.request_id)
            return

        await outbound_queue.put(
            rune_pb2.SessionMessage(
                result=rune_pb2.ExecuteResult(
                    request_id=req.request_id,
                    status=rune_pb2.STATUS_COMPLETED,
                    output=output,
                )
            )
        )

    async def _execute_stream(
        self,
        registered: RegisteredRune,
        ctx: RuneContext,
        req: rune_pb2.ExecuteRequest,
        outbound_queue: asyncio.Queue,
    ) -> None:
        """Execute streaming handler."""

        async def send_event(data: bytes) -> None:
            if req.request_id in self._cancelled:
                raise RuntimeError("cancelled")
            await outbound_queue.put(
                rune_pb2.SessionMessage(
                    stream_event=rune_pb2.StreamEvent(
                        request_id=req.request_id,
                        data=data,
                    )
                )
            )

        sender = StreamSender(send_event)
        if registered.accepts_files:
            attachments = _extract_attachments(req)
            result = registered.handler(ctx, bytes(req.input), attachments, sender)
        else:
            result = registered.handler(ctx, bytes(req.input), sender)
        await _ensure_awaitable(result)

        # Send StreamEnd
        await outbound_queue.put(
            rune_pb2.SessionMessage(
                stream_end=rune_pb2.StreamEnd(
                    request_id=req.request_id,
                    status=rune_pb2.STATUS_COMPLETED,
                )
            )
        )


# ------------------------------------------------------------------
# Helpers (module-level)
# ------------------------------------------------------------------


async def _ensure_awaitable(result):
    """Await a coroutine or return a sync result directly.

    If the handler is a regular (non-async) function its return value
    is *not* a coroutine and attempting ``await`` on it raises
    ``TypeError``.  This wrapper detects that case and returns the
    value as-is, so that sync handlers work transparently.
    """
    if inspect.isawaitable(result):
        return await result
    return result


def _handler_accepts_files(fn: Callable) -> bool:
    """Check if handler function has a 'files' parameter (3rd arg for once, 3rd for stream)."""
    sig = inspect.signature(fn)
    params = list(sig.parameters.keys())
    # Once handler: (ctx, input) or (ctx, input, files)
    # Stream handler: (ctx, input, stream) or (ctx, input, files, stream)
    # If there are >= 3 params, check the 3rd param name
    if len(params) >= 3:
        third = params[2]
        if third == "files":
            return True
    return False


def _extract_attachments(req: rune_pb2.ExecuteRequest) -> list[FileAttachment]:
    """Convert proto attachments to FileAttachment dataclass list."""
    return [
        FileAttachment(
            filename=att.filename,
            data=bytes(att.data),
            mime_type=att.mime_type,
        )
        for att in req.attachments
    ]
