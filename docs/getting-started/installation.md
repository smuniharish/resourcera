# Installation

Python 3.12 or newer is required. From a checkout:

```shell
python -m pip install .
```

After a release is published, the intended distribution name is `resourcera`:

```shell
python -m pip install resourcera
```

For NVIDIA NVML telemetry, explicitly install `resourcera[nvidia]` after release,
or install `.[nvidia]` from a checkout. An appropriate NVIDIA driver and accessible
NVML native library must already be present. Core import never imports the NVIDIA
SDK; no SDK, driver, or package is installed automatically.

For development, install [uv](https://docs.astral.sh/uv/) and run
`uv sync --all-groups`. Trio is a development/test dependency; applications using
the Trio backend should install Trio themselves.
