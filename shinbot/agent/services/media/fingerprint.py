"""Image fingerprint helpers for media semantics and meme handling."""

from __future__ import annotations

import hashlib
import mimetypes
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

from PIL import Image


@dataclass(slots=True)
class MediaFingerprint:
    raw_hash: str
    strict_dhash: str
    file_size: int
    mime_type: str
    width: int | None
    height: int | None
    storage_path: str


def _dhash_channel(pixels: list[int], width: int, hash_size: int) -> str:
    difference: list[bool] = []
    for row in range(hash_size):
        row_offset = row * width
        for col in range(hash_size):
            left = pixels[row_offset + col]
            right = pixels[row_offset + col + 1]
            difference.append(left > right)

    hex_string = ""
    for index in range(0, len(difference), 4):
        chunk = difference[index : index + 4]
        decimal_val = sum(int(bit) << shift for shift, bit in enumerate(chunk))
        hex_string += hex(decimal_val)[2:]
    return hex_string


def _strict_dhash(image: Image.Image, *, hash_size: int = 16) -> str:
    width = hash_size + 1
    height = hash_size
    resized = image.convert("RGB").resize((width, height), Image.Resampling.LANCZOS)
    r_channel, g_channel, b_channel = resized.split()
    return "".join(
        [
            _dhash_channel(list(r_channel.tobytes()), width, hash_size),
            _dhash_channel(list(g_channel.tobytes()), width, hash_size),
            _dhash_channel(list(b_channel.tobytes()), width, hash_size),
        ]
    )


def hamming_distance(hash1: str, hash2: str) -> int:
    if not hash1 or not hash2 or len(hash1) != len(hash2):
        return 999
    return bin(int(hash1, 16) ^ int(hash2, 16)).count("1")


def fingerprint_image_file(path: str | Path) -> MediaFingerprint | None:
    file_path = Path(path).expanduser()
    if not file_path.is_file():
        return None

    data = file_path.read_bytes()
    raw_hash = hashlib.sha256(data).hexdigest()
    file_size = len(data)
    mime_type = mimetypes.guess_type(file_path.name)[0] or ""
    strict_dhash = ""
    width: int | None = None
    height: int | None = None

    try:
        with Image.open(BytesIO(data)) as image:
            width, height = image.size
            strict_dhash = _strict_dhash(image)
            if image.format:
                mime_type = Image.MIME.get(image.format, mime_type)
    except OSError:
        pass

    return MediaFingerprint(
        raw_hash=raw_hash,
        strict_dhash=strict_dhash,
        file_size=file_size,
        mime_type=mime_type,
        width=width,
        height=height,
        storage_path=str(file_path.resolve()),
    )
