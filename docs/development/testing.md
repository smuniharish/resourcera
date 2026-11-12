# Testing and benchmarks

From a checkout:

```shell
uv sync --all-groups
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run pyrefly check
uv run pytest --cov=resourcera --cov-branch
uv run zensical build --strict
uv run python scripts/check_site_links.py
node scripts/render-diagrams.mjs --check
```

To regenerate diagrams, install the pinned renderer locally with
`npm ci --ignore-scripts`, set `PUPPETEER_EXECUTABLE_PATH` to an existing
Chromium or Edge executable, and run `npm run diagrams`. Commit the updated
PNGs and manifest together with their Mermaid sources. `npm run check:diagrams`
checks source, renderer, configuration, image hashes, and documentation references;
the documentation CI job also renders each source to verify Mermaid syntax.
The isolated CI runner disables the browser sandbox because hosted Linux images may
not support it; it has read-only repository permissions and no publishing secrets.
No browser or renderer is installed globally by these commands.

Tests exercise asyncio and Trio, counter warm-up and reset, cgroup hierarchy,
missing/partial providers, budget thresholds, admission races, cancellation,
lease loss, SQLite contention, independent processes, and crash recovery.
Coverage includes normally exiting Python subprocesses, including inventory and
provider workers; terminated children may not write coverage data. Hardware-dependent
platforms still require real-device validation.

Run the latency regression checks with:

```shell
uv run pytest -s -m benchmark tests/test_performance.py
```

The tests enforce cached-snapshot p99 below 5 ms, admission p95 below 150 ms, and
admission p99 below 300 ms on the test runner. These deliberately loose CI bounds
are regression checks, not service-level guarantees. They use a 100% CPU budget
to measure individual operations, rather than asserting sustained default-budget
throughput.

One Windows/Python 3.15 run measured process-local admission p50/p95/p99 of
0.50/0.66/0.72 ms on asyncio and 2.65/3.04/3.26 ms on Trio; SQLite admission
measured 24.06/30.22/32.82 ms and 33.56/38.87/46.41 ms respectively. Results
depend heavily on hardware, filesystem, load, and operating system. A separate
100-second default-policy observation showed temporary suspension during startup
cost accumulation, followed by recovery; this is not a universal performance
claim.
