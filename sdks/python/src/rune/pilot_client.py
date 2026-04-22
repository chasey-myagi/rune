"""Local Pilot discovery and registration helpers."""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import time

from .types import ScalePolicy


_MAX_RESPONSE_SIZE = 256 * 1024  # 256KB — symmetric with daemon's request limit


class PilotClient:
    def __init__(self, pilot_id: str) -> None:
        self.pilot_id = pilot_id

    @classmethod
    async def ensure(cls, runtime: str, api_key: str | None = None) -> "PilotClient":
        normalized = _normalize_runtime(runtime)
        response = None
        try:
            response = await _send_request({"command": "status"})
        except (OSError, TimeoutError):
            pass  # No running pilot or hung socket — will start one below.

        if response is not None:
            status = _classify_status(response, normalized)
            if status[0] == "ready":
                return cls(status[1])
            if status[0] == "failed":
                raise RuntimeError(status[1]) from None
            if status[0] == "mismatch":
                try:
                    await _send_request({"command": "stop"})
                except Exception:
                    pass
            elif status[0] == "retry":
                return await cls._wait_until_ready(normalized, runtime, api_key)

        _start_pilot(runtime, api_key)
        return await cls._wait_until_ready(normalized, runtime, api_key)

    @classmethod
    async def _wait_until_ready(
        cls,
        normalized: str,
        retry_runtime: str | None = None,
        retry_key: str | None = None,
    ) -> "PilotClient":
        """Poll until pilot reports ready. When *retry_runtime* is given,
        re-attempt ``_start_pilot`` on connection failure so a slow
        predecessor release doesn't doom the single initial spawn."""
        deadline = time.monotonic() + _pilot_ensure_timeout()
        last_start = time.monotonic()
        while time.monotonic() < deadline:
            try:
                response = await _send_request({"command": "status"})
            except (OSError, TimeoutError):
                if retry_runtime and time.monotonic() - last_start >= 1.0:
                    _start_pilot(retry_runtime, retry_key)
                    last_start = time.monotonic()
                await asyncio.sleep(0.1)
                continue
            status = _classify_status(response, normalized)
            if status[0] == "ready":
                return cls(status[1])
            if status[0] == "failed":
                raise RuntimeError(status[1]) from None
            await asyncio.sleep(0.1)
        raise RuntimeError("pilot did not become ready") from None

    async def register(self, caster_id: str, policy: ScalePolicy) -> None:
        response = await _send_request(
            {
                "command": "register",
                "caster_id": caster_id,
                "pid": os.getpid(),
                "group": policy.group,
                "spawn_command": policy.spawn_command,
                "shutdown_signal": policy.shutdown_signal,
            }
        )
        _ensure_ok(response)

    async def deregister(self, caster_id: str) -> None:
        response = await _send_request(
            {
                "command": "deregister",
                "caster_id": caster_id,
            }
        )
        _ensure_ok(response)

    @classmethod
    def _from_response(cls, response: dict) -> "PilotClient":
        _ensure_ok(response)
        return cls(str(response["pilot_id"]))

_PILOT_CONNECTING_ERROR = "runtime session not attached"


_DEFAULT_PILOT_ENSURE_TIMEOUT = 5.0
_DEFAULT_PILOT_REQUEST_TIMEOUT = 5.0


def _pilot_ensure_timeout() -> float:
    try:
        return float(os.environ["RUNE_PILOT_ENSURE_TIMEOUT_SECS"])
    except (KeyError, ValueError):
        return _DEFAULT_PILOT_ENSURE_TIMEOUT


def _pilot_request_timeout() -> float:
    try:
        return float(os.environ["RUNE_PILOT_REQUEST_TIMEOUT_SECS"])
    except (KeyError, ValueError):
        return _DEFAULT_PILOT_REQUEST_TIMEOUT


async def _send_request(payload: dict) -> dict:
    return await asyncio.wait_for(_send_request_inner(payload), timeout=_pilot_request_timeout())


async def _send_request_inner(payload: dict) -> dict:
    reader, writer = await asyncio.open_unix_connection(_socket_path())
    writer.write(json.dumps(payload).encode("utf-8"))
    try:
        writer.write_eof()
    except (AttributeError, OSError):
        pass
    await writer.drain()
    data = await reader.read(_MAX_RESPONSE_SIZE + 1)
    writer.close()
    await writer.wait_closed()
    if len(data) > _MAX_RESPONSE_SIZE:
        raise RuntimeError("pilot response exceeded 256KB limit")
    return json.loads(data.decode("utf-8"))


def _ensure_ok(response: dict) -> None:
    if not response.get("ok"):
        raise RuntimeError(response.get("error") or "pilot request failed") from None


def _classify_status(response: dict, normalized: str) -> tuple[str, str]:
    runtime = response.get("runtime", "")
    if runtime != normalized:
        return ("mismatch", "")
    if response.get("ok"):
        return ("ready", str(response["pilot_id"]))
    error = response.get("error")
    if error == _PILOT_CONNECTING_ERROR or not error:
        return ("retry", "")
    return ("failed", error)


def _start_pilot(runtime: str, api_key: str | None) -> None:
    env = dict(os.environ)
    if api_key:
        env["RUNE_KEY"] = api_key
    subprocess.Popen(
        [_find_rune_binary(), "pilot", "daemon", "--runtime", _normalize_runtime(runtime)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        env=env,
    )


def _find_rune_binary() -> str:
    rune_bin = os.environ.get("RUNE_BIN")
    if rune_bin:
        return rune_bin

    found = shutil.which("rune")
    if found:
        return found

    raise RuntimeError("failed to locate rune binary; set RUNE_BIN or add rune to PATH") from None


def _socket_path() -> str:
    return os.path.join(os.path.expanduser("~"), ".rune", "pilot.sock")


def _normalize_runtime(runtime: str) -> str:
    return runtime.strip().rstrip("/")
