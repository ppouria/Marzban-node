import asyncio
import json
import time
from uuid import UUID, uuid4

from fastapi import (APIRouter, Body, FastAPI, HTTPException, Request,
                     WebSocket, status)
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketDisconnect

from config import XRAY_ASSETS_PATH, XRAY_EXECUTABLE_PATH
from logger import logger
from xray import XRayConfig, XRayCore

import requests, platform, zipfile, io, os, stat, shutil
from pathlib import Path

app = FastAPI()


@app.exception_handler(RequestValidationError)
def validation_exception_handler(request: Request, exc: RequestValidationError):
    details = {}
    for error in exc.errors():
        details[error["loc"][-1]] = error.get("msg")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": details}),
    )


class Service(object):
    def __init__(self):
        self.router = APIRouter()

        self.connected = False
        self.client_ip = None
        self.session_id = None
        self.core = XRayCore(
            executable_path=XRAY_EXECUTABLE_PATH,
            assets_path=XRAY_ASSETS_PATH
        )
        self.core_version = self.core.get_version()
        self.config = None

        self.router.add_api_route("/", self.base, methods=["POST"])
        self.router.add_api_route("/ping", self.ping, methods=["POST"])
        self.router.add_api_route("/connect", self.connect, methods=["POST"])
        self.router.add_api_route("/disconnect", self.disconnect, methods=["POST"])
        self.router.add_api_route("/start", self.start, methods=["POST"])
        self.router.add_api_route("/stop", self.stop, methods=["POST"])
        self.router.add_api_route("/restart", self.restart, methods=["POST"])
        self.router.add_api_route("/update_core", self.update_core, methods=["POST"])
        self.router.add_api_route("/update_geo", self.update_geo, methods=["POST"])

        self.router.add_websocket_route("/logs", self.logs)

    def match_session_id(self, session_id: UUID):
        if session_id != self.session_id:
            raise HTTPException(
                status_code=403,
                detail="Session ID mismatch."
            )
        return True

    def response(self, **kwargs):
        return {
            "connected": self.connected,
            "started": self.core.started,
            "core_version": self.core_version,
            **kwargs
        }

    def base(self):
        return self.response()

    def connect(self, request: Request):
        self.session_id = uuid4()
        self.client_ip = request.client.host

        if self.connected:
            logger.warning(
                f'New connection from {self.client_ip}, Core control access was taken away from previous client.')
            if self.core.started:
                try:
                    self.core.stop()
                except RuntimeError:
                    pass

        self.connected = True
        logger.info(f'{self.client_ip} connected, Session ID = "{self.session_id}".')

        return self.response(
            session_id=self.session_id
        )

    def disconnect(self):
        if self.connected:
            logger.info(f'{self.client_ip} disconnected, Session ID = "{self.session_id}".')

        self.session_id = None
        self.client_ip = None
        self.connected = False

        if self.core.started:
            try:
                self.core.stop()
            except RuntimeError:
                pass

        return self.response()

    def ping(self, session_id: UUID = Body(embed=True)):
        self.match_session_id(session_id)
        return {}

    def start(self, session_id: UUID = Body(embed=True), config: str = Body(embed=True)):
        self.match_session_id(session_id)

        try:
            config = XRayConfig(config, self.client_ip)
        except json.decoder.JSONDecodeError as exc:
            raise HTTPException(
                status_code=422,
                detail={
                    "config": f'Failed to decode config: {exc}'
                }
            )

        with self.core.get_logs() as logs:
            try:
                self.core.start(config)

                start_time = time.time()
                end_time = start_time + 3
                last_log = ''
                while time.time() < end_time:
                    while logs:
                        log = logs.popleft()
                        if log:
                            last_log = log
                        if f'Xray {self.core_version} started' in log:
                            break
                    time.sleep(0.1)

            except Exception as exc:
                logger.error(f"Failed to start core: {exc}")
                raise HTTPException(
                    status_code=503,
                    detail=str(exc)
                )

        if not self.core.started:
            raise HTTPException(
                status_code=503,
                detail=last_log
            )

        return self.response()

    def stop(self, session_id: UUID = Body(embed=True)):
        self.match_session_id(session_id)

        try:
            self.core.stop()

        except RuntimeError:
            pass

        return self.response()

    def restart(self, session_id: UUID = Body(embed=True), config: str = Body(embed=True)):
        self.match_session_id(session_id)

        try:
            config = XRayConfig(config, self.client_ip)
        except json.decoder.JSONDecodeError as exc:
            raise HTTPException(
                status_code=422,
                detail={
                    "config": f'Failed to decode config: {exc}'
                }
            )

        try:
            with self.core.get_logs() as logs:
                if self.core.started:
                    try:
                        self.core.stop()
                        time.sleep(0.5)
                    except RuntimeError:
                        pass
                self.core.restart(config)

                start_time = time.time()
                end_time = start_time + 3
                last_log = ''
                while time.time() < end_time:
                    while logs:
                        log = logs.popleft()
                        if log:
                            last_log = log
                        if f'Xray {self.core_version} started' in log:
                            break
                    time.sleep(0.1)

        except Exception as exc:
            logger.error(f"Failed to restart core: {exc}")
            raise HTTPException(
                status_code=503,
                detail=str(exc)
            )

        if not self.core.started:
            raise HTTPException(
                status_code=503,
                detail=last_log
            )

        return self.response()

    async def logs(self, websocket: WebSocket):
        session_id = websocket.query_params.get('session_id')
        interval = websocket.query_params.get('interval')

        try:
            session_id = UUID(session_id)
            if session_id != self.session_id:
                return await websocket.close(reason="Session ID mismatch.", code=4403)

        except ValueError:
            return await websocket.close(reason="session_id should be a valid UUID.", code=4400)

        if interval:
            try:
                interval = float(interval)
            except ValueError:
                return await websocket.close(reason="Invalid interval value", code=4400)

            if interval > 10:
                return await websocket.close(
                    reason="Interval must be more than 0 and at most 10 seconds", code=4400
                )

        await websocket.accept()

        cache = ''
        last_sent_ts = 0
        with self.core.get_logs() as logs:
            while session_id == self.session_id:
                if interval and time.time() - last_sent_ts >= interval and cache:
                    try:
                        await websocket.send_text(cache)
                    except (WebSocketDisconnect, RuntimeError):
                        break
                    cache = ''
                    last_sent_ts = time.time()

                if not logs:
                    try:
                        await asyncio.wait_for(websocket.receive(), timeout=0.2)
                        continue
                    except asyncio.TimeoutError:
                        continue
                    except (WebSocketDisconnect, RuntimeError):
                        break

                log = logs.popleft()

                if interval:
                    cache += f'{log}\n'
                    continue

                try:
                    await websocket.send_text(log)
                except (WebSocketDisconnect, RuntimeError):
                    break

        await websocket.close()

    def _detect_asset_name(self):
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
        raise HTTPException(status_code=400, detail="Unsupported platform for node")

    def _install_zip_to(self, zip_bytes: bytes, target_dir: str):
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
            raise HTTPException(500, detail="xray binary not found in archive")
        try:
            st = os.stat(exe); os.chmod(exe, st.st_mode | stat.S_IEXEC)
        except Exception:
            pass
        return exe

    def _download_files_to(self, path: Path, files: list[dict]) -> list[dict]:
        """
        Download list of {name,url} into the given path.
        Returns list of saved files with absolute path.
        """
        saved = []
        for item in files:
            name = (item.get("name") or "").strip()
            url = (item.get("url") or "").strip()
            if not name or not url:
                raise HTTPException(422, detail="Each file must include non-empty 'name' and 'url'.")
            try:
                r = requests.get(url, timeout=120)
                r.raise_for_status()
            except Exception as e:
                raise HTTPException(502, detail=f"Failed to download {name}: {e}")
            dst = path / name
            try:
                with open(dst, "wb") as f:
                    f.write(r.content)
            except Exception as e:
                raise HTTPException(500, detail=f"Failed to save {name}: {e}")
            saved.append({"name": name, "path": str(dst)})
        return saved

    def _update_docker_compose(self, compose_file: Path, key: str, value: str):
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
            raise HTTPException(500, detail=f"Failed to update docker-compose.yml: {e}")

    def update_core(self, version: str = Body(embed=True)):
        if not version:
            raise HTTPException(422, detail="version is required")

        asset = self._detect_asset_name()
        url = f"https://github.com/XTLS/Xray-core/releases/download/{version}/{asset}"
        try:
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            zip_bytes = r.content
        except Exception as e:
            raise HTTPException(502, detail=f"Download failed: {e}")

        base_dir = Path("/var/lib/reb/xray-core")
        base_dir.mkdir(parents=True, exist_ok=True)
        if self.core.started:
            try:
                self.core.stop()
            except RuntimeError:
                pass
        extracted_exe = Path(self._install_zip_to(zip_bytes, str(base_dir)))
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
        exe_path = str(final_exe)

        self.core.executable_path = exe_path
        self.core_version = self.core.get_version()

        compose_file = Path("/opt/reb/docker-compose.yml")
        if compose_file.exists():
            self._update_docker_compose(compose_file, "XRAY_EXECUTABLE_PATH", "/var/lib/marzban-node/xray-core/xray")

        return {"detail": f"Node core ready at {exe_path}", "version": self.core_version}

    def update_geo(self, files: list = Body(embed=True)):
        """
        Download geo assets to host's mapped volume path and update docker-compose.yml.
        """
        if not isinstance(files, list) or not files:
            raise HTTPException(422, detail="'files' must be a non-empty list of {name,url}.")

        assets_dir = Path("/var/lib/reb/assets")
        assets_dir.mkdir(parents=True, exist_ok=True)
        saved = self._download_files_to(assets_dir, files)

        try:
            self.core.assets_path = "/usr/local/share/xray"
        except Exception:
            pass

        compose_file = Path("/opt/reb/docker-compose.yml")
        if compose_file.exists():
            self._update_docker_compose(compose_file, "XRAY_ASSETS_PATH", "/usr/local/share/xray")

        return {"detail": f"Geo assets saved to {assets_dir}", "saved": saved}


service = Service()
app.include_router(service.router)