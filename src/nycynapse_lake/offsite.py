"""A second home for the backups, off this machine.

Files go through an encrypted rclone remote, so the storage only ever holds ciphertext. They are
copied, never mirrored: emptying the folder here must not empty the folder there. Old copies are
removed only after an upload worked, and only while enough newer ones are there.

The rclone settings (the login and the encryption key) are in RCLONE_CONFIG. The key is also in
its own file, for the owner to keep somewhere else: without it the copies cannot be opened.
"""

import hashlib
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path

from .config import Settings

log = logging.getLogger(__name__)

CONFIG = Path(os.environ.get("RCLONE_CONFIG", "/root/.secrets/nycynapse-rclone.conf"))
REMOTE = os.environ.get("NYC_LAKE_OFFSITE", "offsite:")
APP_BACKUPS = Path(os.environ.get("NYCYNAPSE_APP_BACKUPS", "/root/backups/nycynapse"))
MAX_BYTES = 8 * 1024**3  # stays well inside a free plan
# Folder there, where it comes from here, which files, how many to keep there.
KEEP = {"catalog": 30, "state": 30, "app": 30, "exports": 4}


class NotConfigured(Exception):
    pass


def ready() -> bool:
    """Whether the owner has signed in. The login is the last thing to be filled in."""
    if not CONFIG.exists():
        return False
    text = CONFIG.read_text()
    return "[offsite]" in text and ("token =" in text or "type = local" in text)


def _rclone(*args: str, timeout: int = 3600) -> str:
    done = subprocess.run(
        ["rclone", "--config", str(CONFIG), "--retries", "3", "--low-level-retries", "10", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if done.returncode:
        raise RuntimeError(f"rclone {args[0]} failed: {done.stderr.strip()[-400:]}")
    return done.stdout


def _there(folder: str) -> list[dict]:
    """Files in a folder there, oldest first. A folder that does not exist yet is empty."""
    out = subprocess.run(
        ["rclone", "--config", str(CONFIG), "lsjson", "-R", "--files-only", f"{REMOTE}{folder}"],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    if out.returncode:
        if "directory not found" in out.stderr:
            return []
        raise RuntimeError(f"rclone lsjson failed: {out.stderr.strip()[-400:]}")
    return sorted(json.loads(out.stdout or "[]"), key=lambda f: f["Path"])


def _sources(settings: Settings) -> list[tuple[str, Path, str]]:
    root = settings.backup_path
    out = [
        ("catalog", root, "catalog-*.dump"),
        ("state", root, "state-*.tar.gz"),
        ("app", APP_BACKUPS, "nycynapse-*.dump"),
    ]
    return [s for s in out if s[1].exists()]


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def run(settings: Settings) -> dict:
    if not ready():
        raise NotConfigured("off-site storage is not signed in yet")
    used = sum(f["Size"] for f in _there(""))
    for folder, root, pattern in _sources(settings):
        _rclone("copy", str(root), f"{REMOTE}{folder}", "--include", pattern, "--max-depth", "1")
    exports = settings.backup_path / "exports"
    if exports.exists():
        # A folder still being written ends in .part.
        _rclone("copy", str(exports), f"{REMOTE}exports", "--exclude", "*.part/**")
    removed = prune()
    total = sum(f["Size"] for f in _there(""))
    if total > MAX_BYTES:
        raise RuntimeError(f"{total / 1e9:.1f} GB off-site, more than this should ever hold")
    newest = _there("catalog")[-1]["Path"]
    return {
        "newest": newest,
        "gb": round(total / 1e9, 2),
        "added_mb": round(max(0, total - used) / 1e6, 1),
        "removed": removed,
    }


def prune() -> int:
    n = 0
    for folder, keep in KEEP.items():
        files = _there(folder)
        groups = sorted({f["Path"].split("/")[0] for f in files})
        for name in groups[:-keep]:
            path = f"{REMOTE}{folder}/{name}"
            # An export is a folder of files, everything else one file.
            _rclone("purge" if folder == "exports" else "deletefile", path)
            n += 1
    return n


def check(settings: Settings) -> dict:
    """Bring the newest catalog back, compare it with the one here and restore it for real."""
    from . import backup

    if not ready():
        raise NotConfigured("off-site storage is not signed in yet")
    names = [f["Path"] for f in _there("catalog")]
    if not names:
        raise RuntimeError("there is no catalog off-site")
    work = settings.tmp_path / "offsite-check"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    try:
        _rclone("copy", f"{REMOTE}catalog/{names[-1]}", str(work))
        back = work / names[-1]
        here = settings.backup_path / names[-1]
        if here.exists() and _sha(back) != _sha(here):
            raise RuntimeError(f"{names[-1]} came back different from what was sent")
        return {"from": "off-site", **backup.restore_check(settings, back)}
    finally:
        shutil.rmtree(work, ignore_errors=True)
