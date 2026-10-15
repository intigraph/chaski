#!/usr/bin/env python3
"""Bind a stable release tag, source versions and distribution metadata."""
import argparse
from email.parser import BytesParser
import hashlib
from pathlib import Path
import re
import tarfile
import tomllib
import zipfile


ROOT = Path(__file__).resolve().parents[1]


def source_version(root, tag):
    if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
        raise ValueError("expected a stable vMAJOR.MINOR.PATCH tag")
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    version = tag[1:]
    if project["name"] != "chaski-reactor" or project["version"] != version:
        raise ValueError("tag and project metadata disagree")
    packages = tomllib.loads((root / "uv.lock").read_text())["package"]
    own = [p for p in packages if p["name"] == "chaski-reactor"]
    if len(own) != 1 or own[0]["version"] != version:
        raise ValueError("tag and locked project version disagree")
    return version


def check_metadata(data, version):
    metadata = BytesParser().parsebytes(data)
    if metadata["Name"] != "chaski-reactor" or metadata["Version"] != version:
        raise ValueError("distribution metadata differs from the tagged project")


def distributions(directory, version):
    expected = {
        f"chaski_reactor-{version}-py3-none-any.whl",
        f"chaski_reactor-{version}.tar.gz",
    }
    ignored = {"checksums.txt"}
    build_ignore = directory / ".gitignore"
    if build_ignore.is_file() and build_ignore.read_bytes() == b"*":
        ignored.add(".gitignore")
    actual = {p.name for p in directory.iterdir() if p.name not in ignored}
    if actual != expected:
        raise ValueError("expected exactly one matching wheel and source archive")
    wheel = directory / f"chaski_reactor-{version}-py3-none-any.whl"
    with zipfile.ZipFile(wheel) as archive:
        paths = [p for p in archive.namelist() if p.endswith(".dist-info/METADATA")]
        if len(paths) != 1:
            raise ValueError("wheel must contain exactly one metadata record")
        check_metadata(archive.read(paths[0]), version)
    source = directory / f"chaski_reactor-{version}.tar.gz"
    with tarfile.open(source, "r:gz") as archive:
        paths = [p for p in archive.getmembers() if p.name.count("/") == 1
                 and p.name.endswith("/PKG-INFO") and p.isfile()]
        if len(paths) != 1:
            raise ValueError("source archive must contain one top-level metadata record")
        check_metadata(archive.extractfile(paths[0]).read(), version)
    checksums = "".join(f"{hashlib.sha256((directory / name).read_bytes()).hexdigest()}  {name}\n"
                        for name in sorted(expected))
    (directory / "checksums.txt").write_text(checksums)
    return checksums


def notes(root, version):
    text = (root / "CHANGELOG.md").read_text()
    heading = re.search(rf"^## (?:\[)?{re.escape(version)}(?:\]|\s|$).*$", text, re.MULTILINE)
    if not heading:
        raise ValueError("release notes require the tagged version's changelog section")
    next_heading = re.search(r"^## ", text[heading.end():], re.MULTILINE)
    end = heading.end() + next_heading.start() if next_heading else len(text)
    result = text[heading.start():end].strip()
    if not result.partition("\n")[2].strip():
        raise ValueError("release changelog section is empty")
    return result + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("--dist", type=Path)
    parser.add_argument("--notes", action="store_true")
    args = parser.parse_args()
    try:
        version = source_version(ROOT, args.tag)
        if args.notes:
            print(notes(ROOT, version), end="")
        elif args.dist:
            print(distributions(args.dist, version), end="")
        else:
            print(f"{args.tag}: project and lock versions match")
    except (ValueError, KeyError, OSError, zipfile.BadZipFile, tarfile.TarError) as error:
        parser.exit(1, f"REFUSING release: {error}\n")


if __name__ == "__main__":
    main()
