#!/bin/sh
# Verify the published package before installing; no service or enrollment action.
set -eu
exec python3 - "$@" <<'PYINSTALL'
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen




def fetch(url, destination):
    for attempt in range(3):
        try:
            with urlopen(Request(url, headers={'User-Agent': CONFIG['repo'] + '-installer'}), timeout=60) as response:
                with destination.open('wb') as output:
                    shutil.copyfileobj(response, output)
            return
        except (HTTPError, URLError, TimeoutError) as error:
            transient = not isinstance(error, HTTPError) or error.code in (403, 429, 500, 502, 503, 504)
            if attempt == 2 or not transient:
                raise
            time.sleep(attempt + 1)


def extract(archive, destination):
    with tarfile.open(archive, 'r:gz') as bundle:
        seen, links = set(), []
        total = 0
        for member in bundle:
            name = PurePosixPath(member.name)
            if name.is_absolute() or '..' in name.parts or str(name) in seen:
                raise ValueError('unsafe or duplicate archive path')
            seen.add(str(name))
            target = destination.joinpath(*name.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            if member.isdir():
                target.mkdir(exist_ok=True)
            elif member.isfile():
                total += member.size
                if total > 1024 * 1024 * 1024:
                    raise ValueError('release exceeds 1 GiB unpacked limit')
                source = bundle.extractfile(member)
                if source is None:
                    raise ValueError('missing archive bytes')
                with source, target.open('xb') as output:
                    shutil.copyfileobj(source, output)
                target.chmod(0o755 if member.mode & 0o111 else 0o644)
            elif member.issym():
                link = PurePosixPath(member.linkname)
                if link.is_absolute() or '..' in link.parts:
                    raise ValueError('unsafe archive link')
                links.append((target, member.linkname))
            else:
                raise ValueError('unsupported archive member')
        for target, link in links:
            target.symlink_to(link)
            target.resolve(strict=True).relative_to(destination.resolve())


CONFIG = {'kind': 'wheel', 'package': 'chaski-reactor', 'assets': ['chaski_reactor-{version}-py3-none-any.whl'], 'checksum': 'checksums.txt', 'commands': ['chaski', 'chaski-event', 'chaski-once'], 'repo': 'chaski'}


def bundle_identity(root):
    result = {}
    for path in root.rglob('*'):
        relative = str(path.relative_to(root))
        if relative == '.release.json':
            continue
        if path.is_symlink():
            result[relative] = ('link', os.readlink(path))
        elif path.is_file():
            result[relative] = ('file', path.stat().st_mode & 0o777,
                                hashlib.sha256(path.read_bytes()).hexdigest())
        elif path.is_dir():
            result[relative] = ('directory',)
        else:
            raise ValueError('unexpected installed bundle entry')
    return result


def install():
    if len(sys.argv) > 1 or platform.system() not in ('Linux', 'Darwin'):
        raise ValueError('use a Linux or macOS shell; configure STACK_INSTALL_ROOT through the environment')
    if CONFIG['kind'] == 'wheel' and sys.version_info < (3, 11):
        raise ValueError('Python 3.11 or newer is required')
    with tempfile.TemporaryDirectory(prefix=CONFIG['repo'] + '-release-') as temporary:
        work = Path(temporary)
        tag = os.environ.get('STACK_VERSION')
        if not tag:
            metadata = work / 'latest.json'
            fetch('https://api.github.com/repos/scbrown/' + CONFIG['repo'] + '/releases/latest', metadata)
            release = json.loads(metadata.read_text())
            if release.get('draft') or release.get('prerelease'):
                raise ValueError('latest release is not stable')
            tag = release['tag_name']
        if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', tag):
            raise ValueError('unsupported release tag')
        version = tag[1:]
        base = os.environ.get('STACK_RELEASE_BASE_URL', 'https://github.com/scbrown/' + CONFIG['repo'] + '/releases/download/' + tag)
        manifest = work / 'checksums'
        fetch(base + '/' + CONFIG['checksum'], manifest)
        names = [name.format(tag=tag, version=version) for name in CONFIG['assets']]
        digests = {}
        for name in names:
            matches = [fields[0].lower() for line in manifest.read_text().splitlines()
                       if len(fields := line.split()) == 2 and fields[1].lstrip('*') == name]
            if len(matches) != 1 or not re.fullmatch('[0-9a-f]{64}', matches[0]):
                raise ValueError('missing or ambiguous checksum for ' + name)
            fetch(base + '/' + name, work / name)
            digest = hashlib.sha256((work / name).read_bytes()).hexdigest()
            if digest != matches[0]:
                raise ValueError('checksum mismatch: ' + name)
            digests[name] = digest
        prefix = Path(os.environ.get('STACK_INSTALL_ROOT', str(Path.home() / '.local'))).expanduser().absolute()
        releases = prefix / 'share' / CONFIG['repo']
        releases.mkdir(parents=True, exist_ok=True)
        lock = releases / '.install-lock'
        lock.mkdir()
        destination = releases / tag
        identity = {'tag': tag, 'assets': digests}
        created = False
        try:
            if destination.is_symlink():
                raise ValueError('release destination is a symlink')
            if destination.exists():
                if json.loads((destination / '.release.json').read_text()) != identity:
                    raise ValueError('existing release identity differs; previous install preserved')
                if CONFIG['kind'] == 'source':
                    check = work / 'source-check'
                    check.mkdir()
                    extract(work / names[0], check)
                    if bundle_identity(destination) != bundle_identity(check / ('camayoc-' + tag)):
                        raise ValueError('installed source bytes differ')
                elif CONFIG['kind'] == 'data':
                    for name in names:
                        if hashlib.sha256((destination / name).read_bytes()).hexdigest() != digests[name]:
                            raise ValueError('installed data bytes differ')
            else:
                if CONFIG['kind'] == 'wheel':
                    # The venv is created at its final location so console-script
                    # shebangs remain valid. No command is published before smoke.
                    destination.mkdir()
                    created = True
                    subprocess.run([sys.executable, '-m', 'venv', str(destination)], check=True, timeout=120)
                    python = destination / 'bin/python'
                    subprocess.run([str(python), '-m', 'pip', '--isolated', '--disable-pip-version-check',
                                    'install', '--no-input', str(work / names[0])], check=True, timeout=300)
                    actual = subprocess.check_output([str(python), '-c',
                        'from importlib.metadata import version; print(version(' + repr(CONFIG['package']) + '))'], text=True, timeout=20).strip()
                    if actual != version:
                        raise ValueError('installed package version mismatch')
                    for command in CONFIG['commands']:
                        if not os.access(destination / 'bin' / command, os.X_OK):
                            raise ValueError('missing console entrypoint: ' + command)
                elif CONFIG['kind'] == 'source':
                    unpacked = work / 'source'
                    unpacked.mkdir()
                    extract(work / names[0], unpacked)
                    source = unpacked / ('camayoc-' + tag)
                    actual = json.loads((source / '.claude-plugin/plugin.json').read_text())['version']
                    if actual != version:
                        raise ValueError('source package version mismatch')
                    shutil.copytree(source, destination, symlinks=True)
                    created = True
                else:
                    destination.mkdir()
                    created = True
                    for name in names:
                        data = (work / name).read_bytes()
                        if not data.strip():
                            raise ValueError('empty vocabulary asset')
                        shutil.copyfile(work / name, destination / name)
                (destination / '.release.json').write_text(json.dumps(identity, sort_keys=True) + '\n')
            if CONFIG['kind'] == 'wheel':
                # Pip's RECORD binds installed package/entrypoint bytes. Validate
                # it on both first install and reuse; a stale marker is not proof.
                code = """from importlib.metadata import distribution
from pathlib import Path
import base64, hashlib, sys
root = Path(sys.prefix).resolve()
dist = distribution(sys.argv[1])
assert dist.version == sys.argv[2], 'installed package version mismatch'
for entry in dist.files or []:
    if entry.hash is None:
        continue
    path = Path(dist.locate_file(entry)).resolve()
    path.relative_to(root)
    assert path.is_file(), 'installed package entry is not a regular file'
    actual = base64.urlsafe_b64encode(hashlib.new(entry.hash.mode, path.read_bytes()).digest()).decode().rstrip('=')
    assert actual == entry.hash.value, 'installed package file hash mismatch'
"""
                subprocess.run([str(destination / 'bin/python'), '-c', code,
                                CONFIG['package'], version], check=True, timeout=30)
            if CONFIG['kind'] == 'wheel':
                binaries = prefix / 'bin'
                binaries.mkdir(parents=True, exist_ok=True)
                for command in CONFIG['commands']:
                    if (binaries / command).is_dir():
                        raise ValueError('command destination is a directory')
                for command in CONFIG['commands']:
                    fd, staged = tempfile.mkstemp(prefix='.' + command + '-', dir=binaries)
                    os.close(fd)
                    os.unlink(staged)
                    try:
                        os.symlink(destination / 'bin' / command, staged)
                        os.replace(staged, binaries / command)
                    finally:
                        if os.path.lexists(staged):
                            os.unlink(staged)
            print('Installed verified ' + CONFIG['repo'] + ' ' + version + ' at ' + str(destination))
            if CONFIG['kind'] == 'source':
                print('Source package only. Bootstrap and agent enrollment are separate explicit actions.')
            elif CONFIG['kind'] == 'data':
                print('Vocabulary and shapes only. Nothing was loaded into a running graph.')
            else:
                print('Add ' + str(prefix / 'bin') + ' to PATH. Previous release directories are retained.')
        except BaseException:
            if created:
                shutil.rmtree(destination)
            raise
        finally:
            lock.rmdir()


try:
    install()
except (OSError, ValueError, KeyError, subprocess.SubprocessError, tarfile.TarError) as error:
    print('Install refused: ' + str(error), file=sys.stderr)
    sys.exit(1)
PYINSTALL
