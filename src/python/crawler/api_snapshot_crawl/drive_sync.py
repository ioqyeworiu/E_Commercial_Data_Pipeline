"""
Kho lưu kết quả cho snapshot_crawler.py.

    rclone:gdrive:tiki_crawl_20261118   -> Google Drive qua rclone (cloud routine)
    local:/duong/dan/thu/muc            -> thư mục cục bộ (selftest offline)

Remote rclone cấu hình bằng biến môi trường, không cần file rclone.conf:
    RCLONE_CONFIG_GDRIVE_TYPE=drive
    RCLONE_CONFIG_GDRIVE_SCOPE=drive.file
    RCLONE_CONFIG_GDRIVE_TOKEN={"access_token": ..., "refresh_token": ..., "expiry": ...}
"""
import json
import logging
import os
import shutil
import subprocess
import tempfile
from fnmatch import fnmatch

logger = logging.getLogger("drive_sync")

RCLONE_FLAGS = ["--retries", "5", "--low-level-retries", "20", "--drive-chunk-size", "64M"]


class LocalStore:
    def __init__(self, root):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)
        self.label = f"local:{self.root}"

    def _p(self, rel):
        return os.path.join(self.root, *rel.split("/"))

    def put(self, local_path, rel):
        dst = self._p(rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(local_path, dst)

    def get(self, rel, local_path):
        src = self._p(rel)
        if not os.path.exists(src):
            return False
        os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
        shutil.copyfile(src, local_path)
        return True

    def get_dir(self, rel_dir, local_dir, include=None):
        src = self._p(rel_dir)
        os.makedirs(local_dir, exist_ok=True)
        if not os.path.isdir(src):
            return
        for name in os.listdir(src):
            if include is None or fnmatch(name, include):
                shutil.copyfile(os.path.join(src, name), os.path.join(local_dir, name))

    def exists(self, rel):
        return os.path.exists(self._p(rel))

    def list(self, rel_dir):
        d = self._p(rel_dir)
        return sorted(os.listdir(d)) if os.path.isdir(d) else []

    def put_text(self, rel, text):
        dst = self._p(rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, "w", encoding="utf-8") as f:
            f.write(text)

    def get_text(self, rel):
        p = self._p(rel)
        if not os.path.exists(p):
            return None
        with open(p, encoding="utf-8") as f:
            return f.read()


class RcloneStore:
    def __init__(self, remote):
        self.remote = remote.rstrip("/")          # vd "gdrive:tiki_crawl_20261118"
        self.label = f"rclone:{self.remote}"
        try:
            out = subprocess.run(["rclone", "version"], capture_output=True, text=True, check=True)
        except (OSError, subprocess.CalledProcessError) as e:
            raise RuntimeError("Không tìm thấy rclone trên PATH") from e
        logger.info(out.stdout.splitlines()[0])

    def _r(self, rel):
        return f"{self.remote}/{rel}" if rel else self.remote

    def _run(self, args, input_bytes=None, check=True):
        proc = subprocess.run(["rclone", *args, *RCLONE_FLAGS], input=input_bytes,
                              capture_output=True)
        if check and proc.returncode != 0:
            raise RuntimeError(f"rclone {' '.join(args)} lỗi {proc.returncode}: "
                               f"{proc.stderr.decode('utf-8', 'replace')[-800:]}")
        return proc

    def put(self, local_path, rel):
        self._run(["copyto", local_path, self._r(rel)])

    def get(self, rel, local_path):
        proc = self._run(["copyto", self._r(rel), local_path], check=False)
        return proc.returncode == 0 and os.path.exists(local_path)

    def get_dir(self, rel_dir, local_dir, include=None):
        os.makedirs(local_dir, exist_ok=True)
        args = ["copy", self._r(rel_dir), local_dir]
        if include:
            args += ["--include", include]
        self._run(args, check=False)

    def list(self, rel_dir):
        proc = self._run(["lsjson", "--files-only", self._r(rel_dir)], check=False)
        if proc.returncode != 0:
            return []
        return sorted(item["Name"] for item in json.loads(proc.stdout or b"[]"))

    def exists(self, rel):
        parent, _, name = rel.rpartition("/")
        return name in self.list(parent)

    def put_text(self, rel, text):
        # copyto (không dùng rcat) để Drive ghi đè đúng file cũ thay vì tạo bản trùng tên
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".tmp", delete=False) as f:
            f.write(text)
            tmp = f.name
        try:
            self.put(tmp, rel)
        finally:
            os.unlink(tmp)

    def get_text(self, rel):
        proc = self._run(["cat", self._r(rel)], check=False)
        if proc.returncode != 0:
            return None
        return proc.stdout.decode("utf-8")


def make_store(spec: str):
    if spec.startswith("local:"):
        return LocalStore(spec[len("local:"):])
    if spec.startswith("rclone:"):
        return RcloneStore(spec[len("rclone:"):])
    raise ValueError(f"store không hợp lệ: {spec} (dùng rclone:<remote:path> hoặc local:<dir>)")
