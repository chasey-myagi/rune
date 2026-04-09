"""Local Pilot discovery and registration helpers."""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import time

from .types import ScalePolicy


class PilotClient:
    def __init__(self, pilot_id: str) -> None:
        self.pilot_id = pilot_id

    @classmethod
    async def ensure(cls, runtime: str, api_key: str | None = None) -> "PilotClient":
        try:
            return cls._from_response(await _send_request({"command": "status"}))
        except Exception:
            _start_pilot(runtime, api_key)

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                return cls._from_response(await _send_request({"command": "status"}))
            except Exception:
                await asyncio.sleep(0.1)

        raise RuntimeError("pilot did not become ready")

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


async def _send_request(payload: dict) -> dict:
    reader, writer = await asyncio.open_unix_connection(_socket_path())
    writer.write(json.dumps(payload).encode("utf-8"))
    try:
        writer.write_eof()
    except (AttributeError, OSError):
        pass
    await writer.drain()
    data = await reader.read()
    writer.close()
    await writer.wait_closed()
    return json.loads(data.decode("utf-8"))


def _ensure_ok(response: dict) -> None:
    if not response.get("ok"):
        raise RuntimeError(response.get("error") or "pilot request failed")


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

    raise RuntimeError("failed to locate rune binary; set RUNE_BIN or add rune to PATH")


def _socket_path() -> str:
    return os.path.join(os.path.expanduser("~"), ".rune", "pilot.sock")


def _normalize_runtime(runtime: str) -> str:
    return runtime.strip().rstrip("/")
