# Release checklist

1. Review the changelog, supported Python and dependency ranges, platform
   limitations, and package-name availability. A lookup does not reserve a name.
2. Run the full lint, type, branch-coverage, documentation, examples, diagram,
   lockfile, and cross-platform CI checks.
3. Build the wheel and source distribution; run
   `python scripts/check_distributions.py dist/*.whl dist/*.tar.gz`, validate
   metadata with `twine check`, and test a clean Python 3.12 installation with
   `pip check`.
4. Verify core-only installation, the NVIDIA extra independently, and supported
   combinations without requiring hardware.
5. Obtain explicit authorization before publishing to PyPI or creating a GitHub
   release. No automated workflow in this repository publishes a distribution.
