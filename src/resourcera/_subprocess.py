"""Run bounded, terminable resource probes in child processes."""

from __future__ import annotations

import sys
from subprocess import DEVNULL, PIPE, STDOUT

import anyio

from resourcera.config import ResourceConfig
from resourcera.exceptions import ProviderError


async def run_worker(module: str, config: ResourceConfig, *arguments: str) -> bytes:
    """Return bounded worker output, terminating the child on timeout or cancellation."""
    try:
        process = await anyio.open_process(
            [sys.executable, "-m", module, *arguments],
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=STDOUT,
        )
    except OSError as exc:
        raise ProviderError(f"could not start {module}: {exc}") from exc
    chunks: list[bytes] = []
    size = 0
    limit = min(config.managed_state_budget_bytes, 1024 * 1024)
    try:
        with anyio.fail_after(config.provider_timeout_seconds):
            if process.stdout is None:
                raise ProviderError(f"{module} has no output stream")
            while True:
                try:
                    chunk = await process.stdout.receive(64 * 1024)
                except anyio.EndOfStream:
                    break
                size += len(chunk)
                if size > limit:
                    raise ProviderError(f"{module} exceeded its output budget")
                chunks.append(chunk)
            code = await process.wait()
        output = b"".join(chunks)
        if code != 0:
            raise ProviderError(f"{module} exited with status {code}: {output[:512]!r}")
        return output
    except TimeoutError as exc:
        raise ProviderError(f"{module} timed out") from exc
    finally:
        with anyio.CancelScope(shield=True):
            if process.returncode is None:
                process.kill()
                await process.wait()
            await process.aclose()
