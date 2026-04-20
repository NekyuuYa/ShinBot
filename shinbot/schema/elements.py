"""MessageElement AST and Message container.

Implements the Satori-based message element specification (02_message_element_spec.md).
Messages are represented as ordered sequences of MessageElement nodes forming an AST.

Each MessageElement has:
  - type: tag name (text, at, img, quote, sb:poke, etc.)
  - attrs: attribute dict (content, id, src, etc.)
  - children: nested child elements (for containers like quote, message)
"""

from __future__ import annotations

from typing import Any, Self

from pydantic import BaseModel, Field

# Standard element tags defined in the spec
STANDARD_TAGS: frozenset[str] = frozenset(
    {
        "text",
        "at",
        "sharp",
        "img",
        "emoji",
        "quote",
        "audio",
        "video",
        "file",
        "br",
        "message",
    }
)

# ShinBot extension tags (sb: namespace)
EXTENSION_TAGS: frozenset[str] = frozenset(
    {
        "sb:poke",
        "sb:ark",
    }
)

ALL_KNOWN_TAGS: frozenset[str] = STANDARD_TAGS | EXTENSION_TAGS


class MessageElement(BaseModel):
    """A single node in the message AST.

    Follows the Satori element model: each element has a type (tag name),
    a dict of attributes, and an optional list of child elements for
    container types (quote, message, etc.).
    """

    type: str
    attrs: dict[str, Any] = Field(default_factory=dict)
    children: list[MessageElement] = Field(default_factory=list)

    model_config = {"frozen": False, "extra": "forbid"}

    # ── Factory constructors ────────────────────────────────────────

    @classmethod
    def text(cls, content: str) -> Self:
        return cls(type="text", attrs={"content": content})

    @classmethod
    def at(cls, *, id: str | None = None, name: str | None = None, type: str | None = None) -> Self:
        attrs: dict[str, Any] = {}
        if id is not None:
            attrs["id"] = id
        if name is not None:
            attrs["name"] = name
        if type is not None:
            attrs["type"] = type
        return cls(type="at", attrs=attrs)

    @classmethod
    def sharp(cls, *, id: str, name: str | None = None) -> Self:
        attrs: dict[str, Any] = {"id": id}
        if name is not None:
            attrs["name"] = name
        return cls(type="sharp", attrs=attrs)

    @classmethod
    def img(cls, src: str, **kwargs: Any) -> Self:
        return cls(type="img", attrs={"src": src, **kwargs})

    @classmethod
    def emoji(cls, *, id: str | None = None, name: str | None = None) -> Self:
        attrs: dict[str, Any] = {}
        if id is not None:
            attrs["id"] = id
        if name is not None:
            attrs["name"] = name
        return cls(type="emoji", attrs=attrs)

    @classmethod
    def quote(cls, id: str, children: list[MessageElement] | None = None) -> Self:
        return cls(type="quote", attrs={"id": id}, children=children or [])

    @classmethod
    def audio(cls, src: str, **kwargs: Any) -> Self:
        return cls(type="audio", attrs={"src": src, **kwargs})

    @classmethod
    def video(cls, src: str, **kwargs: Any) -> Self:
        return cls(type="video", attrs={"src": src, **kwargs})

    @classmethod
    def file(cls, src: str, **kwargs: Any) -> Self:
        return cls(type="file", attrs={"src": src, **kwargs})

    @classmethod
    def br(cls) -> Self:
        return cls(type="br")

    # ── Helpers ──────────────────────────────────────────────────────

    @property
    def is_text(self) -> bool:
        return self.type == "text"

    @property
    def text_content(self) -> str:
        """Return text content for text elements, empty string otherwise."""
        if self.type == "text":
            return str(self.attrs.get("content", ""))
        return ""

    def __repr__(self) -> str:
        parts = [f"type={self.type!r}"]
        if self.attrs:
            parts.append(f"attrs={self.attrs!r}")
        if self.children:
            parts.append(f"children=[...{len(self.children)}]")
        return f"MessageElement({', '.join(parts)})"


class Message(BaseModel):
    """An ordered sequence of MessageElement nodes forming a complete message.

    Provides dual-view API:
      - .elements: the AST array for programmatic access
      - .text / get_text(): plain text extraction (concatenated text elements)
      - .to_xml(): Satori XML serialization
    """

    elements: list[MessageElement] = Field(default_factory=list)

    model_config = {"frozen": False, "extra": "forbid"}

    # ── Construction helpers ─────────────────────────────────────────

    @classmethod
    def from_text(cls, text: str) -> Self:
        """Create a Message containing a single text element."""
        return cls(elements=[MessageElement.text(text)])

    @classmethod
    def from_elements(cls, *elements: MessageElement) -> Self:
        return cls(elements=list(elements))

    @classmethod
    def from_xml(cls, xml_content: str) -> Self:
        """Parse Satori XML string into a Message AST.

        Deferred import to avoid circular dependency with satori_parser.
        """
        from shinbot.utils.satori_parser import parse_xml

        return cls(elements=parse_xml(xml_content))

    # ── Dual-view API ────────────────────────────────────────────────

    def get_text(self, *, self_id: str = "") -> str:
        """Extract concatenated plain text from all text elements (recursive)."""
        parts: list[str] = []
        _collect_text(self.elements, parts, self_id=str(self_id or ""))
        return "".join(parts)

    @property
    def text(self) -> str:
        """Alias for get_text() — extract plain text content."""
        return self.get_text()

    def to_xml(self) -> str:
        """Serialize the message AST back to Satori XML.

        Deferred import to avoid circular dependency with satori_parser.
        """
        from shinbot.utils.satori_parser import elements_to_xml

        return elements_to_xml(self.elements)

    # ── Sequence-like interface ──────────────────────────────────────

    def __len__(self) -> int:
        return len(self.elements)

    def __getitem__(self, index: int) -> MessageElement:
        return self.elements[index]

    def __iter__(self):
        return iter(self.elements)

    def __bool__(self) -> bool:
        return len(self.elements) > 0

    def append(self, element: MessageElement) -> None:
        self.elements.append(element)

    def extend(self, elements: list[MessageElement]) -> None:
        self.elements.extend(elements)

    def __add__(self, other: Message) -> Message:
        return Message(elements=self.elements + other.elements)

    def __repr__(self) -> str:
        return f"Message(elements=[...{len(self.elements)}])"


def _collect_text(elements: list[MessageElement], parts: list[str], self_id: str = "") -> None:
    """Recursively collect text content from element tree."""
    for el in elements:
        if el.type == "text":
            parts.append(str(el.attrs.get("content", "")))
        elif el.type == "br":
            parts.append("\n")
        elif el.type == "at":
            parts.append(_format_at_text(el, self_id=self_id))
        elif el.type == "sb:poke":
            target = str(el.attrs.get("target", "") or "").strip()
            if target and self_id and target == self_id:
                parts.append("[戳一戳: 戳了你一下]")
            elif target:
                parts.append(f"[戳一戳: 戳了用户 {target} 一下]")
            else:
                parts.append("[戳一戳]")
        if el.children:
            _collect_text(el.children, parts, self_id)


def _format_at_text(element: MessageElement, *, self_id: str = "") -> str:
    at_type = str(element.attrs.get("type", "") or "").strip().lower()
    if at_type in {"all", "everyone", "here"}:
        return "[@全体成员]"

    target_id = str(element.attrs.get("id", "") or "").strip()
    target_name = str(element.attrs.get("name", "") or "").strip()
    if target_id and self_id and target_id == self_id:
        return "[@你]"
    if target_name and target_id:
        return f"[@{target_name}({target_id})]"
    if target_name:
        return f"[@{target_name}]"
    if target_id:
        return f"[@用户 {target_id}]"
    return "[@某人]"
