# Provider validation

Test provider logic with mocked interfaces and then configure the import path in
`ResourceConfig` and start a `ResourceManager` to exercise child-process import
and serialization. Inspect `snapshot.providers` and `snapshot.accelerators`;
one broken provider must not suppress independent providers.

Cover absent OS interfaces, no devices, multiple stable IDs, partial metrics,
device access errors, a raised exception, a non-importable callable, bounded
responses, and timeouts. Avoid tests that require a physical accelerator. Use
real hardware only when it is available and the user has authorized the relevant
environment; state what was and was not verified.

In the RESOURCERA checkout, `tests/test_accelerators.py` and
`examples/custom_provider.py` provide current contract examples. Run relevant
tests with the project's installed test dependencies; do not install SDKs or
drivers automatically.
