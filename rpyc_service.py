import time
from socket import socket
from threading import Thread

import rpyc

from config import XRAY_ASSETS_PATH, XRAY_EXECUTABLE_PATH
from logger import logger
from xray import XRayConfig, XRayCore

import platform, requests, zipfile, io, os, stat, shutil
from pathlib import Path


class XrayCoreLogsHandler(object):
    def __init__(self, core: XRayCore, callback: callable, interval: float = 0.6):
        self.core = core
        self.callback = callback
        self.interval = interval
        self.active = True
        self.thread = Thread(target=self.cast)
        self.thread.start()

    def stop(self):
        self.active = False
        self.thread.join()

    def cast(self):
        with self.core.get_logs() as logs:
            cache = ''
            last_sent_ts = 0
            while self.active:
                if time.time() - last_sent_ts >= self.interval and cache:
                    self.callback(cache)
                    cache = ''
                    last_sent_ts = time.time()

                if not logs:
                    time.sleep(0.2)
                    continue

                log = logs.popleft()
                cache += f'{log}\n'


@rpyc.service
class XrayService(rpyc.Service):
    def __init__(self):
        self.core = None
        self.connection = None

    def on_connect(self, conn):
        if self.connection:
            try:
                self.connection.ping()
                if self.connection.peer is not None:
                    logger.warning(
                        f'New connection rejected, already connected to {self.connection.peer}')
                return conn.close()
            except (EOFError, TimeoutError, AttributeError):
                if hasattr(self.connection, "peer"):
                    logger.warning(
                        f'Previous connection from {self.connection.peer} has lost')

        peer, _ = socket.getpeername(conn._channel.stream.sock)
        self.connection = conn
        self.connection.peer = peer
        logger.warning(f'Connected to {self.connection.peer}')

    def on_disconnect(self, conn):
        if conn is self.connection:
            logger.warning(f'Disconnected from {self.connection.peer}')

            if self.core is not None:
                self.core.stop()

            self.core = None
            self.connection = None

    @rpyc.exposed
    def start(self, config: str):
        if self.core is not None:
            self.stop()

        try:
            config = XRayConfig(config, self.connection.peer)
            self.core = XRayCore(executable_path=XRAY_EXECUTABLE_PATH,
                                 assets_path=XRAY_ASSETS_PATH)

            if self.connection and hasattr(self.connection.root, 'on_start'):
                @self.core.on_start
                def on_start():
                    try:
                        if self.connection:
                            self.connection.root.on_start()
                    except Exception as exc:
                        logger.debug('Peer on_start exception:', exc)
            else:
                logger.debug(
                    "Peer doesn't have on_start function on it's service, skipped")

            if self.connection and hasattr(self.connection.root, 'on_stop'):
                @self.core.on_stop
                def on_stop():
                    try:
                        if self.connection:
                            self.connection.root.on_stop()
                    except Exception as exc:
                        logger.debug('Peer on_stop exception:', exc)
            else:
                logger.debug(
                    "Peer doesn't have on_stop function on it's service, skipped")

            self.core.start(config)
        except Exception as exc:
            logger.error(exc)
            raise exc

    @rpyc.exposed
    def stop(self):
        if self.core:
            try:
                self.core.stop()
            except RuntimeError:
                pass
        self.core = None

    @rpyc.exposed
    def restart(self, config: str):
        config = XRayConfig(config, self.connection.peer)
        self.core.restart(config)

    @rpyc.exposed
    def fetch_xray_version(self):
        if self.core is None:
            raise ProcessLookupError("Xray has not been started")

        return self.core.version

    @rpyc.exposed
    def fetch_logs(self, callback: callable) -> XrayCoreLogsHandler:
        if self.core:
            logs = XrayCoreLogsHandler(self.core, callback)
            logs.exposed_stop = logs.stop
            logs.exposed_cast = logs.cast
            return logs

    @staticmethod
    def _detect_asset_name():
        sys = platform.system().lower()
        arch = platform.machine().lower()
        if sys.startswith("linux"):
            if arch in ("x86_64", "amd64"):
                return "Xray-linux-64.zip"
            if arch in ("aarch64", "arm64"):
                return "Xray-linux-arm64-v8a.zip"
            if arch in ("armv7l", "armv7"):
                return "Xray-linux-arm32-v7a.zip"
            if arch in ("armv6l",):
                return "Xray-linux-arm32-v6.zip"
            if arch in ("riscv64",):
                return "Xray-linux-riscv64.zip"
        raise RuntimeError("Unsupported platform for node")

    @staticmethod
    def _install_zip(zip_bytes: bytes, target_dir: str) -> str:
        os.makedirs(target_dir, exist_ok=True)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            z.extractall(target_dir)
        exe = os.path.join(target_dir, "xray")
        if platform.system().lower().startswith("windows"):
            exe = os.path.join(target_dir, "xray.exe")
        if not os.path.exists(exe):
            alt = os.path.join(target_dir, "Xray")
            alt_win = os.path.join(target_dir, "Xray.exe")
            exe = alt if os.path.exists(alt) else (alt_win if os.path.exists(alt_win) else exe)
        if not os.path.exists(exe):
            raise RuntimeError("xray binary not found")
        try:
            st = os.stat(exe); os.chmod(exe, st.st_mode | stat.S_IEXEC)
        except Exception:
            pass
        return exe

    @staticmethod
    def _download_files_to(path: Path, files: list[dict]) -> list[dict]:
        """
        Download list of {name,url} into the given path.
        Returns list of saved files with absolute path.
        """
        saved = []
        for item in files:
            name = (item.get("name") or "").strip()
            url = (item.get("url") or "").strip()
            if not name or not url:
                raise RuntimeError("Each item must include non-empty 'name' and 'url'")
            try:
                r = requests.get(url, timeout=120)
                r.raise_for_status()
            except Exception as e:
                raise RuntimeError(f"Failed to download {name}: {e}")
            dst = path / name
            try:
                with open(dst, "wb") as f:
                    f.write(r.content)
            except Exception as e:
                raise RuntimeError(f"Failed to save {name}: {e}")
            saved.append({"name": name, "path": str(dst)})
        return saved

    @staticmethod
    def _update_docker_compose(compose_file: Path, key: str, value: str):
        """Update or add an environment variable in docker-compose.yml and restart container."""
        try:
            with open(compose_file, "r") as f:
                content = f.read()
            
            import yaml
            data = yaml.safe_load(content) or {"services": {"marzban-node": {"environment": {}}}}
            env = data.get("services", {}).get("marzban-node", {}).get("environment", {})
            
            env[key] = value
            
            volumes = data.get("services", {}).get("marzban-node", {}).get("volumes", [])
            asset_volume = "/var/lib/reb/assets:/usr/local/share/xray"
            if asset_volume not in volumes:
                volumes.append(asset_volume)
            data["services"]["marzban-node"]["environment"] = env
            data["services"]["marzban-node"]["volumes"] = volumes
            
            with open(compose_file, "w") as f:
                yaml.safe_dump(data, f, allow_unicode=True)
            
            subprocess.run(["docker-compose", "-f", str(compose_file), "up", "-d"], check=True)
        except Exception as e:
            raise RuntimeError(f"Failed to update docker-compose.yml: {e}")

    @rpyc.exposed
    def update_core(self, version: str):
        asset = self._detect_asset_name()
        url = f"https://github.com/XTLS/Xray-core/releases/download/{version}/{asset}"
        try:
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            zip_bytes = r.content
        except Exception as e:
            raise RuntimeError(f"Download failed: {e}")

        base_dir = Path("/var/lib/reb/xray-core")
        base_dir.mkdir(parents=True, exist_ok=True)
        if self.core is not None and self.core.started:
            try:
                self.core.stop()
            except RuntimeError:
                pass
        extracted_exe = Path(self._install_zip(zip_bytes, str(base_dir)))
        final_exe = base_dir / "xray"
        try:
            if extracted_exe != final_exe:
                if final_exe.exists():
                    final_exe.unlink()
                extracted_exe.rename(final_exe)
        except Exception:
            shutil.copyfile(str(extracted_exe), str(final_exe))
            if platform.system().lower().startswith("linux"):
                final_exe.chmod(final_exe.stat().st_mode | stat.S_IEXEC)
        exe = str(final_exe)

        if self.core is not None:
            self.core.executable_path = exe
            try:
                _ = self.core.get_version()
            except Exception:
                pass

        compose_file = Path("/opt/reb/docker-compose.yml")
        if compose_file.exists():
            self._update_docker_compose(compose_file, "XRAY_EXECUTABLE_PATH", "/var/lib/marzban-node/xray-core/xray")

        return {"detail": f"Node core ready at {exe}"}

    @rpyc.exposed
    def update_geo(self, files: list):
        """
        Download geo assets to host's mapped volume path and update docker-compose.yml.
        """
        if not isinstance(files, list) or not files:
            raise RuntimeError("files must be a non-empty list of {name,url}")

        assets_dir = Path("/var/lib/reb/assets")
        assets_dir.mkdir(parents=True, exist_ok=True)
        saved = self._download_files_to(assets_dir, files)

        if self.core is not None:
            try:
                self.core.assets_path = "/usr/local/share/xray"
            except Exception:
                pass

        compose_file = Path("/opt/reb/docker-compose.yml")
        if compose_file.exists():
            self._update_docker_compose(compose_file, "XRAY_ASSETS_PATH", "/usr/local/share/xray")

        return {"detail": f"Geo assets saved to {assets_dir}", "saved": saved}