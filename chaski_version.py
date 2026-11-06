"""Shared console version identity from the installed distribution metadata."""
from importlib import metadata


def add_version_argument(parser, program):
    """Add an argparse early exit without requiring operational inputs."""
    try:
        version = metadata.version("chaski-reactor")
    except metadata.PackageNotFoundError:
        version = "source checkout"
    parser.add_argument("--version", action="version", version=f"{program} {version}")
