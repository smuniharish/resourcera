# Contributing

Keep RESOURCERA a standalone resource-intelligence library. Add tests and documentation
for user-visible behavior, report unavailable measurements honestly, and do not commit
credentials or local environments.

Use Python 3.12 or later and uv:

```shell
uv sync --all-groups
uv run ruff check .
uv run ruff format --check .
uv run pyrefly check
uv run pytest --cov=resourcera --cov-branch
uv run zensical build --strict
uv run python scripts/check_site_links.py
npm ci --ignore-scripts
node scripts/render-diagrams.mjs --check
```

For diagram regeneration, install the pinned project-local Node dependencies and set
`PUPPETEER_EXECUTABLE_PATH` to an existing Chromium or Edge executable. Run
`node scripts/render-diagrams.mjs` and commit the sources, rendered images, and manifest
together. Never install vendor SDKs or hardware drivers from library code.

Report security issues privately as described in [SECURITY.md](SECURITY.md).
