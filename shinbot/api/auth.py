"""JWT authentication utilities for the ShinBot management API."""

from __future__ import annotations

import logging
import os
import secrets
import time
from pathlib import Path
from typing import Any, Literal, cast

import jwt

try:
    import bcrypt

    _HAS_BCRYPT = True
except ImportError:
    _HAS_BCRYPT = False
    logging.getLogger(__name__).warning(
        "bcrypt is not installed; passwords will be stored and compared as plaintext. "
        "Install with: uv add bcrypt"
    )

_BCRYPT_PREFIX = "$2b$"

logger = logging.getLogger(__name__)


class AuthConfig:
    """Holds admin credentials and JWT configuration.

    JWT secret priority:
      1. ``[admin].jwt_secret`` in data/config.toml
      2. ``data/admin_secret.key`` auto-generated on first start
    """

    ALGORITHM = "HS256"
    # Sentinel values used only to detect whether the operator has ever
    # changed credentials away from the factory defaults.  These are NOT
    # injected as actual credentials anywhere in the codebase; on first boot
    # a cryptographically random password is generated instead (see boot.py).
    DEFAULT_USERNAME = "admin"
    DEFAULT_PASSWORD = "admin"

    def __init__(self, config: dict[str, Any], data_dir: Path) -> None:
        admin_cfg = config.get("admin", {})
        self.username: str = admin_cfg.get("username", self.DEFAULT_USERNAME)
        # Prefer password_hash (bcrypt) over plaintext password for migration
        password_hash = admin_cfg.get("password_hash", "")
        password_plain = admin_cfg.get("password", self.DEFAULT_PASSWORD)
        self._password: str = password_hash if password_hash else password_plain
        self.jwt_expire_hours: int = int(admin_cfg.get("jwt_expire_hours", 24))
        self.session_cookie_name: str = (
            str(admin_cfg.get("auth_cookie_name", "shinbot_session")).strip()
            or "shinbot_session"
        )
        self.session_cookie_path: str = (
            str(admin_cfg.get("auth_cookie_path", "/")).strip() or "/"
        )
        session_cookie_domain = str(admin_cfg.get("auth_cookie_domain", "")).strip()
        self.session_cookie_domain: str | None = session_cookie_domain or None
        self.session_cookie_samesite: Literal["lax", "strict", "none"] = self._normalize_samesite(
            admin_cfg.get("auth_cookie_samesite", "strict")
        )
        self._session_cookie_secure = self._coerce_optional_bool(
            admin_cfg.get("auth_cookie_secure")
        )

        secret_from_cfg: str = admin_cfg.get("jwt_secret", "")
        self.jwt_secret: str = (
            secret_from_cfg if secret_from_cfg else self._load_or_create_secret(data_dir)
        )

    # ── Credential verification ──────────────────────────────────────

    def verify_password(self, username: str, password: str) -> bool:
        """Verify username and password using constant-time comparison."""
        # Use constant-time comparison to prevent timing attacks.
        username_ok = secrets.compare_digest(username, self.username)
        if self._password.startswith(_BCRYPT_PREFIX):
            if not _HAS_BCRYPT:
                logger.error("Cannot verify bcrypt hash: bcrypt is not installed")
                return False
            password_ok = bcrypt.checkpw(password.encode(), self._password.encode())
        else:
            password_ok = secrets.compare_digest(password, self._password)
        return username_ok and password_ok

    def is_using_default_credentials(self) -> bool:
        """Check if credentials are still factory defaults (admin/admin)."""
        return self.username == self.DEFAULT_USERNAME and self._password == self.DEFAULT_PASSWORD

    def set_credentials(self, username: str, password: str) -> None:
        """Update the in-memory admin credentials, hashing the password with bcrypt."""
        self.username = username
        self._password = self._hash_password(password)

    @staticmethod
    def _hash_password(password: str) -> str:
        """Hash a password with bcrypt. Falls back to plaintext if bcrypt is unavailable."""
        if _HAS_BCRYPT:
            return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        return password

    # ── Token lifecycle ──────────────────────────────────────────────

    def create_token(self, subject: str | None = None) -> str:
        """Create a JWT token for the given subject."""
        now = int(time.time())
        payload = {
            "sub": subject or self.username,
            "username": self.username,
            "iat": now,
            "exp": now + self.jwt_expire_hours * 3600,
        }
        return jwt.encode(payload, self.jwt_secret, algorithm=self.ALGORITHM)

    def decode_token(self, token: str) -> dict[str, Any]:
        """Decode and validate a JWT.  Raises jwt.InvalidTokenError on failure."""
        return jwt.decode(token, self.jwt_secret, algorithms=[self.ALGORITHM])

    @property
    def session_cookie_max_age(self) -> int:
        """Return the session cookie max-age in seconds, derived from the JWT expiry."""
        return max(self.jwt_expire_hours, 1) * 3600

    def is_secure_cookie(self, scheme: str | None = None) -> bool:
        """Determine whether the session cookie should be flagged as ``Secure``.

        If an explicit override was set via configuration it is honoured
        directly.  Otherwise the decision is inferred from the request
        scheme — ``True`` for HTTPS/WSS, ``False`` otherwise.

        Args:
            scheme: The request URL scheme (e.g. ``"https"``).
        """
        if self._session_cookie_secure is not None:
            return self._session_cookie_secure
        return (scheme or "").lower() in {"https", "wss"}

    # ── Internal helpers ─────────────────────────────────────────────

    @staticmethod
    def _coerce_optional_bool(value: Any) -> bool | None:
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return True
            if normalized in {"false", "0", "no", "off"}:
                return False
        return bool(value)

    @staticmethod
    def _normalize_samesite(value: Any) -> Literal["lax", "strict", "none"]:
        normalized = str(value or "strict").strip().lower()
        if normalized in {"lax", "strict", "none"}:
            return cast(Literal["lax", "strict", "none"], normalized)
        return "strict"

    def _load_or_create_secret(self, data_dir: Path) -> str:
        secret_file = data_dir / "admin_secret.key"
        if secret_file.exists():
            return secret_file.read_text("utf-8").strip()
        secret = secrets.token_hex(32)
        secret_file.write_text(secret, "utf-8")
        # Restrict to owner-read-only; JWT secret must not be world-readable.
        os.chmod(secret_file, 0o600)
        return secret
