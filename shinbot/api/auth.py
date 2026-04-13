"""JWT authentication utilities for the ShinBot management API."""

from __future__ import annotations

import secrets
import time
from pathlib import Path
from typing import Any

import jwt


class AuthConfig:
    """Holds admin credentials and JWT configuration.

    JWT secret priority:
      1. ``[admin].jwt_secret`` in config.toml
      2. ``data/admin_secret.key`` auto-generated on first start
    """

    ALGORITHM = "HS256"

    def __init__(self, config: dict[str, Any], data_dir: Path) -> None:
        admin_cfg = config.get("admin", {})
        self.username: str = admin_cfg.get("username", "admin")
        self._password: str = admin_cfg.get("password", "admin")
        self.jwt_expire_hours: int = int(admin_cfg.get("jwt_expire_hours", 24))

        secret_from_cfg: str = admin_cfg.get("jwt_secret", "")
        self.jwt_secret: str = (
            secret_from_cfg if secret_from_cfg else self._load_or_create_secret(data_dir)
        )

    # ── Credential verification ──────────────────────────────────────

    def verify_password(self, username: str, password: str) -> bool:
        return username == self.username and password == self._password

    # ── Token lifecycle ──────────────────────────────────────────────

    def create_token(self) -> str:
        now = int(time.time())
        payload = {
            "sub": "admin",
            "iat": now,
            "exp": now + self.jwt_expire_hours * 3600,
        }
        return jwt.encode(payload, self.jwt_secret, algorithm=self.ALGORITHM)

    def decode_token(self, token: str) -> dict:
        """Decode and validate a JWT.  Raises jwt.InvalidTokenError on failure."""
        return jwt.decode(token, self.jwt_secret, algorithms=[self.ALGORITHM])

    # ── Internal helpers ─────────────────────────────────────────────

    def _load_or_create_secret(self, data_dir: Path) -> str:
        secret_file = data_dir / "admin_secret.key"
        if secret_file.exists():
            return secret_file.read_text("utf-8").strip()
        secret = secrets.token_hex(32)
        secret_file.write_text(secret, "utf-8")
        return secret
