"""Release boundaries reject mismatched or ambiguous artifacts."""
import importlib.util
import io
from pathlib import Path
import tarfile
import zipfile

import pytest

spec = importlib.util.spec_from_file_location(
    "release_artifacts", Path(__file__).parent / "scripts/release-artifacts.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def make_distributions(root, metadata_version="0.1.0"):
    metadata = f"Name: chaski-reactor\nVersion: {metadata_version}\n".encode()
    with zipfile.ZipFile(root / "chaski_reactor-0.1.0-py3-none-any.whl", "w") as archive:
        archive.writestr("chaski_reactor-0.1.0.dist-info/METADATA", metadata)
    with tarfile.open(root / "chaski_reactor-0.1.0.tar.gz", "w:gz") as archive:
        info = tarfile.TarInfo("chaski_reactor-0.1.0/PKG-INFO")
        info.size = len(metadata)
        archive.addfile(info, io.BytesIO(metadata))


def test_tag_is_bound_to_project_and_lock(tmp_path):
    (tmp_path / "pyproject.toml").write_text('[project]\nname="chaski-reactor"\nversion="0.1.0"\n')
    (tmp_path / "uv.lock").write_text('[[package]]\nname="chaski-reactor"\nversion="0.2.0"\n')
    with pytest.raises(ValueError, match="locked project"):
        release.source_version(tmp_path, "v0.1.0")
    with pytest.raises(ValueError, match="stable"):
        release.source_version(tmp_path, "main")


def test_checksums_bind_actual_artifacts_and_reject_extra_payload(tmp_path):
    make_distributions(tmp_path)
    (tmp_path / ".gitignore").write_bytes(b"*")
    before = release.distributions(tmp_path, "0.1.0")
    with zipfile.ZipFile(tmp_path / "chaski_reactor-0.1.0-py3-none-any.whl", "a") as archive:
        archive.writestr("payload.py", "changed = True\n")
    assert release.distributions(tmp_path, "0.1.0") != before
    (tmp_path / "unexpected.whl").touch()
    with pytest.raises(ValueError, match="exactly one"):
        release.distributions(tmp_path, "0.1.0")


def test_distribution_version_and_release_notes_refuse_false_identity(tmp_path):
    make_distributions(tmp_path, metadata_version="9.9.9")
    with pytest.raises(ValueError, match="metadata"):
        release.distributions(tmp_path, "0.1.0")
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n\n## 0.2.0\n\nChanges\n")
    with pytest.raises(ValueError, match="changelog section"):
        release.notes(tmp_path, "0.1.0")
    (tmp_path / "CHANGELOG.md").write_text(
        "# Changelog\n\n## [0.1.0](https://example.com)\n\nFirst release\n\n## 0.0.1\n\nOld\n")
    assert "First release" in release.notes(tmp_path, "0.1.0")
    assert "Old" not in release.notes(tmp_path, "0.1.0")
