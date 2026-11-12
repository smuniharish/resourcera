"""Verify local links and anchors in the generated documentation site."""

from __future__ import annotations

import sys
import tomllib
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: set[str] = set()
        self.references: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        for key in ("id", "name"):
            if attributes.get(key):
                self.anchors.add(attributes[key] or "")
        for key in ("href", "src"):
            if attributes.get(key):
                self.references.append(attributes[key] or "")


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    site = (root / "site").resolve()
    config = tomllib.loads((root / "zensical.toml").read_text(encoding="utf-8"))
    prefix = urlsplit(config["project"]["site_url"]).path.rstrip("/")
    pages = sorted(site.rglob("*.html"))
    if not pages:
        print("No generated HTML found; build the documentation first.", file=sys.stderr)
        return 1
    parsed: dict[Path, PageParser] = {}
    for path in pages:
        parser = PageParser()
        parser.feed(path.read_text(encoding="utf-8"))
        parsed[path.resolve()] = parser

    failures: list[str] = []
    checked = 0
    for page, parser in parsed.items():
        for reference in parser.references:
            url = urlsplit(reference)
            if url.scheme or url.netloc:
                continue
            path = unquote(url.path)
            if path.startswith("/") and prefix:
                if path == prefix:
                    path = "/"
                elif path.startswith(prefix + "/"):
                    path = path[len(prefix) :]
                else:
                    failures.append(f"{page.relative_to(site)}: link outside site: {reference}")
                    continue
            target = site / path.lstrip("/") if path.startswith("/") else page.parent / path
            target = target.resolve() if path else page
            try:
                target.relative_to(site)
            except ValueError:
                failures.append(f"{page.relative_to(site)}: link escapes site: {reference}")
                continue
            if target.is_dir():
                target = target / "index.html"
            checked += 1
            if not target.is_file():
                failures.append(f"{page.relative_to(site)}: missing {reference}")
            elif url.fragment and target in parsed:
                anchor = unquote(url.fragment)
                if anchor not in parsed[target].anchors:
                    failures.append(f"{page.relative_to(site)}: missing anchor {reference}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"Validated {checked} local links and assets across {len(pages)} pages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
