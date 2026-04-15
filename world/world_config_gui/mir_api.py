"""MiR REST API helpers for world_config_gui."""

from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import requests


MIR_DEFAULT_BASE_URL = "http://192.168.12.20"
MIR_DEFAULT_API_PREFIX = "/api/v2.0.0"
MIR_DEFAULT_TIMEOUT_S = 4.0


class MirApiError(RuntimeError):
    """Raised when the MiR API cannot be reached or returns invalid data."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass
class MirApiClient:
    """Small HTTP client wrapper around the MiR REST API."""

    base_url: str
    timeout_s: float = MIR_DEFAULT_TIMEOUT_S
    verify_tls: bool = False
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    auth_variants: Tuple[str, ...] = ("sha256", "plain")

    @classmethod
    def from_env(cls) -> "MirApiClient":
        return cls.from_settings(None)

    @classmethod
    def from_settings(cls, settings: Optional[Dict[str, Any]]) -> "MirApiClient":
        cfg = settings or {}
        base_url = _cfg_or_env_str(cfg, "base_url", "MIR_BASE_URL", MIR_DEFAULT_BASE_URL)
        api_prefix = _cfg_or_env_str(cfg, "api_prefix", "MIR_API_PREFIX", MIR_DEFAULT_API_PREFIX) or MIR_DEFAULT_API_PREFIX
        timeout_raw = _cfg_or_env_str(cfg, "timeout_s", "MIR_TIMEOUT_S", str(MIR_DEFAULT_TIMEOUT_S))
        verify_tls = _cfg_or_env_bool(cfg, "verify_tls", "MIR_VERIFY_TLS", default=False)
        username = _cfg_or_env_optional_str(cfg, "username", "MIR_USERNAME")
        password = _cfg_or_env_optional_str(cfg, "password", "MIR_PASSWORD")
        api_key = _cfg_or_env_optional_str(cfg, "api_key", "MIR_API_KEY") or _cfg_or_env_optional_str(cfg, "api_key", "MIR_AUTH_TOKEN")
        raw_variants = _cfg_or_env_str(cfg, "auth_variants", "MIR_AUTH_VARIANTS", "sha256,plain")
        auth_variants = _parse_auth_variants(raw_variants)

        try:
            timeout_s = float(timeout_raw)
        except Exception:
            timeout_s = MIR_DEFAULT_TIMEOUT_S
        if timeout_s <= 0:
            timeout_s = MIR_DEFAULT_TIMEOUT_S

        return cls(
            base_url=_normalize_base_url(base_url, api_prefix),
            timeout_s=timeout_s,
            verify_tls=verify_tls,
            username=username,
            password=password,
            api_key=api_key,
            auth_variants=auth_variants,
        )

    @property
    def origin(self) -> str:
        parsed = urlsplit(self.base_url)
        return f"{parsed.scheme}://{parsed.netloc}"

    # ------------------------------------------------------------------
    # Base resources
    # ------------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        data = self._request_json("GET", "/status")
        if not isinstance(data, dict):
            raise MirApiError("MiR /status response was not a JSON object")
        return data

    def get_maps(self) -> List[Dict[str, Any]]:
        data = self._request_json("GET", "/maps")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            for key in ("results", "items", "data"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    def get_map(self, map_id: str) -> Dict[str, Any]:
        data = self._request_json("GET", f"/maps/{map_id}")
        if not isinstance(data, dict):
            raise MirApiError(f"MiR /maps/{map_id} response was not a JSON object")
        return data

    def get_position(self, position_ref: str) -> Dict[str, Any]:
        ref = str(position_ref or "").strip()
        if not ref:
            raise MirApiError("Position reference is empty")
        if ref.startswith("/"):
            path = ref
        else:
            path = f"/positions/{ref}"
        data = self._request_json("GET", path)
        if not isinstance(data, dict):
            raise MirApiError(f"MiR position response for '{position_ref}' was not a JSON object")
        return data

    def get_map_positions(self, map_id: str, include_details: bool = True) -> List[Dict[str, Any]]:
        data = self._request_json("GET", f"/maps/{map_id}/positions")
        if isinstance(data, list):
            basic_positions = [item for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            value = data.get("positions")
            basic_positions = [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
        else:
            basic_positions = []

        if not include_details:
            return basic_positions

        detailed: List[Dict[str, Any]] = []
        for item in basic_positions:
            pos_ref = str(item.get("url") or item.get("guid") or "").strip()
            if not pos_ref:
                continue
            try:
                detail = self.get_position(pos_ref)
            except MirApiError:
                detail = {}
            merged = dict(item)
            if isinstance(detail, dict):
                merged.update(detail)
            detailed.append(merged)
        return detailed

    # ------------------------------------------------------------------
    # Pose and map helpers
    # ------------------------------------------------------------------

    def resolve_map_id(self, map_id: Optional[str]) -> str:
        if map_id and map_id.strip():
            return map_id.strip()
        status = self.get_status()
        status_map_id = self.extract_map_id(status)
        if not status_map_id:
            raise MirApiError("Could not resolve map_id (missing in MiR status)")
        return status_map_id

    @staticmethod
    def extract_map_id(status: Dict[str, Any]) -> Optional[str]:
        for key in ("map_id", "map_guid"):
            value = status.get(key)
            if value is None:
                continue
            out = str(value).strip()
            if out:
                return out
        return None

    @staticmethod
    def extract_pose(status: Dict[str, Any]) -> Dict[str, Optional[float]]:
        def pick(d: Dict[str, Any], *keys: str) -> Optional[float]:
            for k in keys:
                if k in d:
                    v = _to_float(d.get(k))
                    if v is not None:
                        return v
            return None

        pos_candidates = (
            status.get("position"),
            status.get("pose"),
            status.get("robot_position"),
        )
        for pos in pos_candidates:
            if not isinstance(pos, dict):
                continue
            x = pick(pos, "x", "pos_x", "position_x")
            y = pick(pos, "y", "pos_y", "position_y")
            orientation = pick(pos, "orientation", "theta", "heading", "yaw", "angle")
            if x is not None or y is not None or orientation is not None:
                return {"x": x, "y": y, "orientation": orientation}

        return {
            "x": pick(status, "x", "pos_x", "position_x"),
            "y": pick(status, "y", "pos_y", "position_y"),
            "orientation": pick(status, "orientation", "theta", "heading", "yaw", "angle", "position_orientation"),
        }

    @staticmethod
    def extract_map_image_data_url(map_payload: Dict[str, Any]) -> Optional[str]:
        for field in ("base_map", "map", "image_data"):
            raw = map_payload.get(field)
            if not isinstance(raw, str):
                continue
            value = raw.strip()
            if not value:
                continue
            if value.startswith("data:image/"):
                return value
            if _looks_like_url(value):
                continue
            data_url = _base64_to_data_url(value)
            if data_url:
                return data_url
        return None

    def get_map_image_binary(
        self,
        map_id: str,
        map_payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[bytes, str]]:
        """Try common MiR endpoints for binary map data and return bytes + content type."""

        candidates: List[str] = [
            f"/maps/{map_id}/map",
            f"/maps/{map_id}/image",
            f"/maps/{map_id}/base_map",
        ]
        if map_payload:
            for field in ("image_url", "image", "map", "base_map", "url"):
                value = map_payload.get(field)
                if isinstance(value, str) and value.strip() and _looks_like_url(value.strip()):
                    candidates.append(value.strip())

        seen = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            try:
                resp = self._request_raw("GET", candidate, accept="image/*,application/octet-stream,*/*")
            except MirApiError:
                continue
            content = resp.content or b""
            if not content:
                continue
            content_type = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
            if content_type.startswith("image/") or _looks_like_image_bytes(content):
                return content, (content_type or "image/png")

            if "json" in content_type:
                try:
                    obj = resp.json()
                except Exception:
                    obj = None
                if isinstance(obj, dict):
                    data_url = self.extract_map_image_data_url(obj)
                    if data_url:
                        decoded = self.decode_data_url(data_url)
                        if decoded:
                            return decoded
        return None

    @staticmethod
    def decode_data_url(value: str) -> Optional[Tuple[bytes, str]]:
        if not isinstance(value, str) or not value.startswith("data:"):
            return None
        if "," not in value:
            return None
        head, payload = value.split(",", 1)
        media_type = "application/octet-stream"
        if head.startswith("data:"):
            meta = head[5:]
            if ";" in meta:
                media_type = meta.split(";", 1)[0] or media_type
            elif meta:
                media_type = meta
        if ";base64" in head:
            try:
                decoded = base64.b64decode(payload, validate=False)
            except Exception:
                return None
            return decoded, media_type
        return None

    # ------------------------------------------------------------------
    # Raw request helpers
    # ------------------------------------------------------------------

    def _request_json(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Any:
        resp = self._request_raw(method, path, params=params, accept="application/json", json_body=json_body)
        try:
            return resp.json()
        except Exception as exc:
            raise MirApiError(f"MiR response was not valid JSON for path '{path}'") from exc

    def _request_raw(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        accept: Optional[str] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        url = self._build_url(path)
        headers = {"Accept-Language": "en_US"}
        if accept:
            headers["Accept"] = accept
        if self.api_key:
            headers["Authorization"] = self.api_key

        auth_headers = self._build_authorization_headers()
        if not auth_headers:
            auth_headers = [None]

        last_resp: Optional[requests.Response] = None
        for auth_header in auth_headers:
            request_headers = dict(headers)
            if auth_header:
                request_headers["Authorization"] = auth_header
            try:
                resp = requests.request(
                    method=method,
                    url=url,
                    headers=request_headers,
                    params=params,
                    json=json_body,
                    timeout=self.timeout_s,
                    verify=self.verify_tls,
                )
            except requests.RequestException as exc:
                raise MirApiError(f"Failed to call MiR API '{url}': {exc}") from exc

            last_resp = resp
            # Retry next auth variant only on 401.
            if resp.status_code == 401:
                continue
            break

        assert last_resp is not None
        resp = last_resp
        if resp.status_code >= 400:
            body_preview = (resp.text or "").strip().replace("\n", " ")
            if len(body_preview) > 240:
                body_preview = body_preview[:240] + "..."
            raise MirApiError(
                f"MiR API returned HTTP {resp.status_code} for '{url}': {body_preview}",
                status_code=resp.status_code,
            )
        return resp

    def update_position(self, position_ref: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        ref = str(position_ref or "").strip()
        if not ref:
            raise MirApiError("Position reference is empty")
        if ref.startswith("/"):
            path = ref
        else:
            path = f"/positions/{ref}"
        data = self._request_json("PUT", path, json_body=payload)
        if not isinstance(data, dict):
            raise MirApiError(f"MiR position update response for '{position_ref}' was not a JSON object")
        return data

    def _build_authorization_headers(self) -> List[str]:
        if self.api_key:
            # Explicit API key/token has priority and is used as-is.
            return [self.api_key]
        if self.username is None:
            return []
        password = self.password or ""
        out: List[str] = []
        for variant in self.auth_variants:
            if variant == "plain":
                raw = f"{self.username}:{password}"
            elif variant == "sha256":
                sha = hashlib.sha256(password.encode("utf-8")).hexdigest()
                raw = f"{self.username}:{sha}"
            else:
                continue
            token = base64.b64encode(raw.encode("utf-8")).decode("ascii")
            out.append(f"Basic {token}")
        return out

    def _build_url(self, path: str) -> str:
        candidate = (path or "").strip()
        if not candidate:
            return self.base_url
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return candidate
        if candidate.startswith("/api/"):
            return self.origin + candidate
        if candidate.startswith("/v2"):
            return self.origin + "/api" + candidate
        if candidate.startswith("/"):
            return self.base_url + candidate
        return self.base_url + "/" + candidate


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_str(name: str) -> Optional[str]:
    raw = os.getenv(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _cfg_or_env_str(cfg: Dict[str, Any], cfg_key: str, env_key: str, default: str) -> str:
    env_val = os.getenv(env_key)
    if env_val is not None and str(env_val).strip():
        return str(env_val).strip()
    raw = cfg.get(cfg_key, default)
    return str(raw).strip() if raw is not None else str(default).strip()


def _cfg_or_env_optional_str(cfg: Dict[str, Any], cfg_key: str, env_key: str) -> Optional[str]:
    env_val = os.getenv(env_key)
    if env_val is not None:
        v = str(env_val).strip()
        return v or None
    raw = cfg.get(cfg_key)
    if raw is None:
        return None
    v = str(raw).strip()
    return v or None


def _cfg_or_env_bool(cfg: Dict[str, Any], cfg_key: str, env_key: str, default: bool) -> bool:
    env_val = os.getenv(env_key)
    if env_val is not None:
        return str(env_val).strip().lower() in {"1", "true", "yes", "on"}
    raw = cfg.get(cfg_key)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_base_url(base_url: str, api_prefix: str) -> str:
    base = (base_url or MIR_DEFAULT_BASE_URL).strip()
    if not base.startswith(("http://", "https://")):
        base = "http://" + base
    base = base.rstrip("/")

    normalized_prefix = api_prefix.strip() or MIR_DEFAULT_API_PREFIX
    if not normalized_prefix.startswith("/"):
        normalized_prefix = "/" + normalized_prefix
    normalized_prefix = normalized_prefix.rstrip("/")

    # If caller already gave full API root, keep it.
    if "/api/" in base:
        return base
    if base.endswith(normalized_prefix):
        return base
    return base + normalized_prefix


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _looks_like_url(value: str) -> bool:
    s = value.strip()
    return s.startswith("http://") or s.startswith("https://") or s.startswith("/")


def _base64_to_data_url(value: str) -> Optional[str]:
    raw = "".join(value.split())
    if len(raw) < 256:
        return None
    # Pad in case provider strips "=" chars.
    if len(raw) % 4:
        raw += "=" * (4 - (len(raw) % 4))
    try:
        decoded = base64.b64decode(raw, validate=False)
    except Exception:
        return None
    if len(decoded) < 32:
        return None
    media_type = "image/png"
    if decoded.startswith(b"\xff\xd8\xff"):
        media_type = "image/jpeg"
    elif decoded.startswith(b"GIF87a") or decoded.startswith(b"GIF89a"):
        media_type = "image/gif"
    elif decoded.startswith(b"BM"):
        media_type = "image/bmp"
    return f"data:{media_type};base64,{raw}"


def _looks_like_image_bytes(content: bytes) -> bool:
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return True
    if content.startswith(b"\xff\xd8\xff"):
        return True
    if content.startswith(b"GIF87a") or content.startswith(b"GIF89a"):
        return True
    if content.startswith(b"BM"):
        return True
    return False


def _parse_auth_variants(raw: str) -> Tuple[str, ...]:
    items = [x.strip().lower() for x in (raw or "").split(",")]
    allowed = {"sha256", "plain"}
    out = tuple(x for x in items if x in allowed)
    if out:
        return out
    return ("sha256", "plain")
