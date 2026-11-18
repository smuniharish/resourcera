# Release checklist

1. Review the changelog, supported Python and dependency ranges, platform
   limitations, and package-name availability. A lookup does not reserve a name.
2. Run the full lint, type, branch-coverage, documentation, examples, diagram,
   lockfile, and cross-platform CI checks.
3. Build the wheel and source distribution and inspect their contents and metadata:

   ```shell
   uv build
   uv run --locked --no-default-groups --group package twine check "dist/*.whl" "dist/*.tar.gz"
   uv run --locked --no-default-groups --group package python scripts/check_distributions.py "dist/*.whl" "dist/*.tar.gz"
   ```

   Inspect the artifact metadata and test a clean Python 3.12 installation with
   `pip check`. Keep only the intended release artifacts in `dist/` before upload.
4. Verify core-only installation, the NVIDIA extra independently, and supported
   combinations without requiring hardware.
5. After maintainer approval, upload the reviewed artifacts manually with
   `uv run --locked --no-default-groups --group package twine upload "dist/*"`.
   Create the GitHub release manually and verify the version and release notes.
   No workflow in this repository publishes a distribution.
