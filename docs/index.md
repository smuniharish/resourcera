---
hide:
  - navigation
  - toc
---

<div class="rc-hero" markdown>

<span class="rc-hero__eyebrow">Resource-aware Python</span>

# Know your resources.<br>Admit work with confidence.

<p class="rc-hero__lead">
Observe local resources, estimate workload capacity, and coordinate bounded work
with an async-first library that fits into your application.
</p>

<div class="rc-hero__actions" markdown>

[Get started](getting-started/index.md){ .md-button .md-button--primary }
[Explore the guide](guide/index.md){ .md-button }

</div>
</div>

<div class="rc-section-heading" markdown>

## From observations to decisions

RESOURCERA combines established system telemetry with conservative, explainable
resource reasoning. Use the pieces you need without adopting a framework.

</div>

<div class="grid cards" markdown>

-   :material-chart-line: **See what's available**

    ---

    Inspect CPU, memory, disk, network, containers, and visible accelerators with
    explicit status, source, and freshness.

    [Explore metrics :material-arrow-right:](guide/metrics.md)

-   :material-scale-balance: **Estimate capacity**

    ---

    Describe the resources a workload needs and see which dimension limits
    concurrency, including when a required measurement is unknown.

    [Understand capacity :material-arrow-right:](guide/capacity.md)

-   :material-shield-check-outline: **Admit bounded work**

    ---

    Make explainable admission decisions, keep a lease while work runs, and
    handle rejection or finite waiting explicitly.

    [Learn about admission :material-arrow-right:](guide/admission.md)

-   :material-connection: **Coordinate locally**

    ---

    Share logical reservations in one interpreter or among cooperating processes
    on the same machine.

    [Review the guarantees :material-arrow-right:](guide/coordination.md)

</div>

## A small, async-first API

Create a manager, inspect a snapshot, and protect the lifetime of your work:

```python
import anyio

from resourcera import AdmissionRejectedError, ResourceManager


async def main():
    async with ResourceManager() as resources:
        snapshot = await resources.snapshot()
        print("Memory status:", snapshot.memory.available_bytes.status)
        try:
            async with resources.admission({"memory": "64MiB"}) as decision:
                print("Admitted:", decision.lease_id)
                await anyio.sleep(0.01)
        except AdmissionRejectedError as error:
            print("Not admitted:", error.decision.reason)


anyio.run(main)
```

[Try the full quickstart :material-arrow-right:](getting-started/quickstart.md)

## Built around clear boundaries

<div class="rc-architecture" markdown>

![RESOURCERA resource intelligence architecture](assets/diagrams/overall-architecture.png)

</div>

Measurements are observations, not promises. Unknown or stale required data is not
free capacity; logical reservations do not provide physical isolation. Explore the
[architecture](guide/architecture.md), [sampling and overhead](guide/sampling-overhead.md),
and [accelerator limitations](guide/accelerators.md) for the details.

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

## Find your next step

<div class="grid cards" markdown>

-   :material-rocket-launch-outline: **Start building**

    [Installation](getting-started/installation.md) ·
    [Quickstart](getting-started/quickstart.md) ·
    [Examples](examples/index.md)

-   :material-book-open-page-variant-outline: **Go deeper**

    [User guide](guide/index.md) ·
    [API reference](api/index.md) ·
    [Architecture diagrams](architecture/diagrams.md)

-   :material-robot-outline: **Work with coding agents**

    [Agent skills](agent-skills/index.md) ·
    [Skill installation](agent-skills/install.md)

</div>
