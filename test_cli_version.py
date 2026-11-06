"""Version inspection must exit before any operational arguments or I/O."""
import contextlib
import importlib
from importlib import metadata
import io
import sys
from unittest import mock

import pytest

COMMANDS = [
    ("chaski", "chaski"), ("external_event", "chaski-event"),
    ("once", "chaski-once"), ("github_routes", "chaski-github-routes"),
    ("github_work", "chaski-github-work"),
    ("state_key_migration", "chaski-state-key-migrate"),
]


@pytest.mark.parametrize("module,program", COMMANDS)
def test_version_without_operational_arguments(module, program):
    entry = importlib.import_module(module)
    output = io.StringIO()
    with mock.patch.object(sys, "argv", [program, "--version"]), contextlib.redirect_stdout(output):
        with pytest.raises(SystemExit) as result:
            entry.main()
    assert result.value.code == 0
    assert output.getvalue() == program + " " + metadata.version("chaski-reactor") + "\n"
    assert len(output.getvalue().splitlines()) == 1


@pytest.mark.parametrize("module,program", COMMANDS)
def test_operational_arguments_still_required(module, program):
    entry = importlib.import_module(module)
    with mock.patch.object(sys, "argv", [program]):
        with pytest.raises(SystemExit) as result:
            entry.main()
    assert result.value.code == 2
