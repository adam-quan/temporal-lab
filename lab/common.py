"""Shared config, Runtime (Prometheus metrics) and Client construction."""
from __future__ import annotations

import os
import socket
from typing import Optional

from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig

ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "lab")
DEFAULT_TASK_QUEUE = os.environ.get("TASK_QUEUE", "lab-tq")


def env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    """Env var as string; unset OR empty -> default (compose passes '' for unset knobs)."""
    v = os.environ.get(name, "")
    return v if v != "" else default


def env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    v = env_str(name)
    return int(v) if v is not None else default


def env_float(name: str, default: Optional[float] = None) -> Optional[float]:
    v = env_str(name)
    return float(v) if v is not None else default


def identity(prefix: str) -> str:
    """Worker/client identity shown in event history (`identity` field) and the UI."""
    return f"{prefix}@{socket.gethostname()}:{os.getpid()}"


def make_runtime(metrics_port: Optional[int]) -> Optional[Runtime]:
    """Create the SDK Runtime with a Prometheus endpoint, or None for the default runtime.

    Must be created before the Client/Worker. Histograms are in MILLISECONDS
    (Core SDK default) and counters have no `_total` suffix, so metric names
    look like `temporal_activity_schedule_to_start_latency_bucket`.
    """
    if not metrics_port:
        return None
    return Runtime(
        telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address=f"0.0.0.0:{metrics_port}"))
    )


async def connect(
    metrics_port: Optional[int] = None,
    address: str = ADDRESS,
    namespace: str = NAMESPACE,
    id_prefix: str = "client",
    **connect_kwargs,
) -> Client:
    runtime = make_runtime(metrics_port)
    kwargs = {"namespace": namespace, "identity": identity(id_prefix), **connect_kwargs}
    if runtime is not None:
        kwargs["runtime"] = runtime
    return await Client.connect(address, **kwargs)
