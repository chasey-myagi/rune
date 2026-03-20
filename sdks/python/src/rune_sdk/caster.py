"""Caster -- connects to Rune runtime and executes registered handlers."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Awaitable

import grpc

from .types import RuneConfig, RuneContext
from .stream import StreamSender
from .handler import RegisteredRune, OnceHandler

# Ensure _proto dir is on sys.path before importing generated code.
from . import _proto  # noqa: F401
from ._proto.rune.wire.v1 import rune_pb2, rune_pb2_grpc

logger = logging.getLogger("rune_sdk")


class Caster:
    """Connects to a Rune runtime server and executes registered handlers."""

    def __init__(
        self,
        addr: str,
        caster_id: str = "python-caster",
        max_concurrent: int = 10,
    ) -> None:
        self._addr = addr
        self._caster_id = caster_id
        self._max_concurrent = max_concurrent
        self._runes: dict[str, RegisteredRune] = {}
        self._cancelled: set[str] = set()
        self._reconnect_base_delay = 1.0  # seconds
        self._reconnect_max_delay = 30.0

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
            )
            self._runes[name] = RegisteredRune(config=config, handler=fn, is_stream=False)
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
            )
            self._runes[name] = RegisteredRune(config=config, handler=fn, is_stream=True)
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the caster (blocking). Runs asyncio event loop with auto-reconnect."""
        asyncio.run(self._run_with_reconnect())

    async def _run_with_reconnect(self) -> None:
        """Reconnect loop with exponential backoff."""
        delay = self._reconnect_base_delay
        while True:
            try:
                await self._session()
                # Session ended normally (detach) -- don't reconnect
                logger.info("session ended normally")
                break
            except grpc.aio.AioRpcError as e:
                logger.warning("gRPC error: %s, reconnecting in %.1fs", e.code(), delay)
            except Exception as e:
                logger.warning("session error: %s, reconnecting in %.1fs", e, delay)

            await asyncio.sleep(delay)
            delay = min(delay * 2, self._reconnect_max_delay)
            logger.info("reconnecting to %s ...", self._addr)

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------

    async def _session(self) -> None:
        """Run one gRPC session."""
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

            # Build attach message
            declarations = []
            for registered in self._runes.values():
                decl = rune_pb2.RuneDeclaration(
                    name=registered.config.name,
                    version=registered.config.version,
                    description=registered.config.description,
                    supports_stream=registered.config.supports_stream,
                )
                if registered.config.gate:
                    decl.gate.CopyFrom(
                        rune_pb2.GateConfig(
                            path=registered.config.gate,
                            method=registered.config.gate_method,
                        )
                    )
                declarations.append(decl)

            attach_msg = rune_pb2.SessionMessage(
                attach=rune_pb2.CasterAttach(
                    caster_id=self._caster_id,
                    runes=declarations,
                    max_concurrent=self._max_concurrent,
                )
            )
            await outbound_queue.put(attach_msg)

            # Start heartbeat task
            hb_task = asyncio.create_task(self._heartbeat_loop(outbound_queue))

            try:
                async for msg in call:
                    payload = msg.WhichOneof("payload")

                    if payload == "attach_ack":
                        if msg.attach_ack.accepted:
                            logger.info(
                                "attached to %s, caster_id=%s",
                                self._addr,
                                self._caster_id,
                            )
                        else:
                            logger.error("attach rejected: %s", msg.attach_ack.reason)
                            break

                    elif payload == "execute":
                        req = msg.execute
                        asyncio.create_task(self._handle_execute(req, outbound_queue))

                    elif payload == "cancel":
                        self._cancelled.add(msg.cancel.request_id)
                        logger.info("cancel requested: %s", msg.cancel.request_id)

                    elif payload == "heartbeat":
                        pass  # server heartbeat received

            finally:
                hb_task.cancel()
                await outbound_queue.put(None)  # stop outbound iter

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self, queue: asyncio.Queue) -> None:
        """Send periodic heartbeat."""
        try:
            while True:
                await asyncio.sleep(10)
                msg = rune_pb2.SessionMessage(
                    heartbeat=rune_pb2.Heartbeat(
                        timestamp_ms=int(time.time() * 1000),
                    )
                )
                await queue.put(msg)
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
        """Execute a rune handler (once or stream)."""
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

    async def _execute_once(
        self,
        registered: RegisteredRune,
        ctx: RuneContext,
        req: rune_pb2.ExecuteRequest,
        outbound_queue: asyncio.Queue,
    ) -> None:
        """Execute unary handler."""
        output = await registered.handler(ctx, bytes(req.input))

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
        await registered.handler(ctx, bytes(req.input), sender)

        # Send StreamEnd
        await outbound_queue.put(
            rune_pb2.SessionMessage(
                stream_end=rune_pb2.StreamEnd(
                    request_id=req.request_id,
                    status=rune_pb2.STATUS_COMPLETED,
                )
            )
        )
