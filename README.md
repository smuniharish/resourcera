<p align="center">
  <img src="https://raw.githubusercontent.com/smuniharish/resourcera/main/docs/assets/logo.svg" alt="RESOURCERA resource gauge logo" width="112" height="112">
</p>

<h1 align="center">RESOURCERA</h1>

<p align="center">
  <strong>Know your resources. Admit work with confidence.</strong><br>
  Resource intelligence, capacity estimation, and resource-aware concurrency for Python applications.
</p>

<p align="center">
  <a href="https://github.com/smuniharish/resourcera/actions/workflows/ci.yml"><img src="https://github.com/smuniharish/resourcera/actions/workflows/ci.yml/badge.svg" alt="CI status"></a>
  <a href="https://resourcera.readthedocs.io/"><img src="https://readthedocs.org/projects/resourcera/badge/?version=latest" alt="Documentation status"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3.12%20%7C%203.13%20%7C%203.14%20%7C%203.15-3b4cca" alt="Python 3.12, 3.13, 3.14 and 3.15"></a>
  <img src="https://img.shields.io/badge/typing-typed-6655b8" alt="Typed package">
  <a href="https://github.com/smuniharish/resourcera/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-257b74" alt="Apache License 2.0"></a>
  <a href="https://github.com/smuniharish/resourcera"><img src="https://img.shields.io/badge/asyncio%20%2B%20Trio-supported-6655b8" alt="Asyncio and Trio supported"></a>
</p>

<p align="center">
  <a href="https://resourcera.readthedocs.io/">Documentation</a> ·
  <a href="https://github.com/smuniharish/resourcera/blob/main/docs/getting-started/quickstart.md">Quickstart</a> ·
  <a href="https://github.com/smuniharish/resourcera/tree/main/examples">Examples</a> ·
  <a href="https://github.com/smuniharish/resourcera/tree/main/skills">Agent skills</a> ·
  <a href="https://github.com/smuniharish/resourcera/blob/main/CONTRIBUTING.md">Contributing</a>
</p>

---

RESOURCERA is an embeddable, async-first library that observes local resources,
estimates capacity for a workload, and helps cooperating application tasks make
explainable admission decisions. It is framework-neutral and does not require an
accelerator SDK.

## What you can do

| Capability | How it helps |
| --- | --- |
| **Observe** | Inspect CPU, memory, disk, network, container limits, and visible accelerators with explicit freshness and capability states. |
| **Estimate** | Describe workload requirements and identify the limiting resource without treating missing data as free capacity. |
| **Admit** | Bound concurrent work with explainable acceptance or rejection and optional finite waiting. |
| **Adapt** | Replace hardcoded semaphores, limiters, and thread-pool sizes with resource-derived, self-adjusting concurrency limits. |
| **Coordinate** | Hold logical leases within an interpreter or between cooperating processes on one machine. |
| **Control overhead** | Use bounded, adaptive sampling with observable self-overhead safeguards. |

RESOURCERA is not a dashboard, distributed execution engine, or physical resource
isolation mechanism. Measurements and estimates are observations, not guarantees.

## Installation

Python 3.12, 3.13, 3.14, and 3.15 are supported (3.12 or newer is required).
Install from PyPI:

```shell
python -m pip install resourcera
```

For optional NVIDIA NVML telemetry, install `resourcera[nvidia]` and provide a
compatible driver and NVML library. To install from a checkout, use
`python -m pip install .`. RESOURCERA never installs SDKs or drivers.

## Quickstart

```python
import anyio

from resourcera import AdmissionRejectedError, ResourceManager


async def main():
    async with ResourceManager(scope="machine") as resources:
        snapshot = await resources.snapshot()
        print("Available memory:", snapshot.memory.available_bytes.value, "bytes")

        try:
            async with resources.admission({"memory": "64MiB"}) as decision:
                print("Admitted:", decision.lease_id)
                await anyio.sleep(0.01)  # Your workload goes here.
        except AdmissionRejectedError as error:
            print("Not admitted:", error.decision.reason)


anyio.run(main)
```

The manager owns its sampling and lease lifecycle. CPU and throughput rates need
two samples; unavailable, stale, or failed required measurements reject admission.
Keep the admission context open while the work runs.

### Adaptive concurrency

Instead of `asyncio.Semaphore(100)` or `ThreadPoolExecutor(max_workers=10)`, let
capacity decide:

```python
async with resources.adaptive_concurrency(
    {"cpu": 0.5, "memory": "256MiB"}, max_concurrency=50
) as controller:
    async with controller.slot():
        await do_work()

recommendation = await resources.recommend_concurrency(
    {"memory": "64MiB", "profile": "io-bound"}, max_concurrency=32
)
executor = ThreadPoolExecutor(max_workers=recommendation.recommended_limit)
```

Each slot holds a real lease. Limits fall immediately when capacity drops and
rise cautiously, and unknown capacity is never treated as unlimited. See the
[adaptive concurrency guide](https://resourcera.readthedocs.io/en/latest/guide/concurrency/).

## How it fits together

![RESOURCERA resource intelligence architecture](https://raw.githubusercontent.com/smuniharish/resourcera/main/docs/assets/diagrams/overall-architecture.png)

Start with the [quickstart](https://resourcera.readthedocs.io/en/latest/getting-started/quickstart/),
then explore [capacity estimates](https://resourcera.readthedocs.io/en/latest/guide/capacity/),
[admission](https://resourcera.readthedocs.io/en/latest/guide/admission/),
and [coordination guarantees](https://resourcera.readthedocs.io/en/latest/guide/coordination/). Platform-specific
accelerator capabilities and their limitations are described in the
[accelerator guide](https://resourcera.readthedocs.io/en/latest/guide/accelerators/).

## Agent skills

The repository includes two optional [Agent Skills](https://github.com/smuniharish/resourcera/tree/main/skills):
`resourcera` for using the public API safely, and `resourcera-provider` for
building isolated accelerator providers. They are instructions for compatible
coding agents, not runtime dependencies. See the
[installation guide](https://resourcera.readthedocs.io/en/latest/agent-skills/install/).

## Downloads

<p>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527resourcera%2527%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=downloads&amp;color=blue&amp;cacheSeconds=3600" alt="Total downloads"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520date%2520%253E%253D%2520toStartOfWeek%2528today%2528%2529%252C%25201%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=this%20week&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads this week"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520date%2520%253E%253D%2520toStartOfMonth%2528today%2528%2529%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=this%20month&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads this month"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520sum%2528count%2529%2520v%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520date%2520%253E%253D%2520toStartOfYear%2528today%2528%2529%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=this%20year&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads this year"></a>
</p>

<p>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528toString%2528y%2529%252C%2520%2527%253A%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520toYear%2528date%2529%2520y%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day%2520WHERE%2520project%253D%2527resourcera%2527%2520GROUP%2520BY%2520y%2520ORDER%2520BY%2520y%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=by%20year&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads by year"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%2520LIMIT%25205%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=top%20countries&amp;color=blue&amp;cacheSeconds=3600" alt="Top five download countries"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527A%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20A&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries A"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527B%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20B&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries B"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527C%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20C&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries C"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527D%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20D&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries D"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527E%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20E&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries E"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527F%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20F&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries F"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527G%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20G&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries G"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527H%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20H&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries H"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527I%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20I&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries I"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527J%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20J&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries J"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527K%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20K&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries K"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527L%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20L&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries L"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527M%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20M&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries M"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520left%2528country_code%252C%25201%2529%2520%253E%253D%2520%2527N%2527%2520AND%2520left%2528country_code%252C%25201%2529%2520%253C%253D%2520%2527O%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20N-O&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries N-O"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527P%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20P&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries P"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527R%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20R&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries R"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527S%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20S&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries S"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520startsWith%2528country_code%252C%2520%2527T%2527%2529%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20T&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries T"></a>
  <a href="https://clickpy.clickhouse.com/dashboard/resourcera"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fsql-clickhouse.clickhouse.com%2F%3Fuser%3Ddemo%26query%3DSELECT%2520arrayStringConcat%2528groupArray%2528concat%2528country_code%252C%2520%2527%2520%2527%252C%2520toString%2528c%2529%2529%2529%252C%2520%2527%2520%257C%2520%2527%2529%2520v%2520FROM%2520%2528SELECT%2520country_code%252C%2520sum%2528count%2529%2520c%2520FROM%2520pypi.pypi_downloads_per_day_by_version_by_country%2520WHERE%2520project%253D%2527resourcera%2527%2520AND%2520left%2528country_code%252C%25201%2529%2520%253E%253D%2520%2527U%2527%2520AND%2520left%2528country_code%252C%25201%2529%2520%253C%253D%2520%2527Y%2527%2520GROUP%2520BY%2520country_code%2520ORDER%2520BY%2520c%2520DESC%252C%2520country_code%2529%2520FORMAT%2520JSON&amp;query=%24.data%5B0%5D.v&amp;label=countries%20U-Y&amp;color=blue&amp;cacheSeconds=3600" alt="Downloads in countries U-Y"></a>
</p>

Download data comes from the public ClickHouse PyPI dataset, which refreshes daily,
so counts can be about a day behind. Shields caches badge images for up to an hour,
and GitHub may cache them longer. Weeks start on Monday in UTC. Country groups
without downloads are omitted and need a documentation update if they become active.
Explore interactive charts on the
[ClickPy dashboard](https://clickpy.clickhouse.com/dashboard/resourcera).

## Project

- [Documentation](https://resourcera.readthedocs.io/) and [examples](https://github.com/smuniharish/resourcera/tree/main/examples)
- [Contributing](https://github.com/smuniharish/resourcera/blob/main/CONTRIBUTING.md),
  [security](https://github.com/smuniharish/resourcera/blob/main/SECURITY.md), and
  [changelog](https://github.com/smuniharish/resourcera/blob/main/CHANGELOG.md)
- Licensed under the [Apache License 2.0](https://github.com/smuniharish/resourcera/blob/main/LICENSE)
