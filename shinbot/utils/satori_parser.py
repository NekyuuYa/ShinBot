"""Satori XML ⇌ MessageElement AST bidirectional serializer.

Handles parsing Satori protocol XML (as received from adapters) into
MessageElement AST arrays, and serializing AST back to XML strings.

Key behaviors:
  - Bare text outside tags → MessageElement(type="text")
  - Nested elements (e.g. <quote>...<img/></quote>) → children list
  - Entity escaping in both directions (&amp; &lt; &gt; &quot;)
  - sb: namespace extension tags preserved as-is
  - Tolerant of malformed input (falls back to text element)
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from lxml import etree

if TYPE_CHECKING:
    from collections.abc import Sequence

from shinbot.models.elements import MessageElement

# ── XML → AST ───────────────────────────────────────────────────────

# Namespace map for lxml parsing
_NSMAP = {"sb": "urn:shinbot:elements"}
_SYNTHETIC_NS_PREFIX = "urn:shinbot:prefixed:"

# Self-closing tags that never have meaningful children
_VOID_TAGS = frozenset({"br", "img", "audio", "video", "file", "emoji"})


def parse_xml(xml_content: str) -> list[MessageElement]:
    """Parse a Satori XML string into a list of MessageElement nodes.

    The input may contain:
      - Bare text: ``"hello world"``
      - Mixed content: ``"hello <at id='123'/> world"``
      - Nested containers: ``"<quote id='1'>text<img src='x'/></quote>"``
      - Extension tags: ``"<sb:poke target='123' type='shake'/>"``
      - Platform-specific tags: ``"<llonebot:ark data='...'/>"``

    Returns an ordered list of top-level MessageElement nodes.
    """
    if not xml_content or not xml_content.strip():
        return []

    # Wrap in a root element so lxml can parse mixed content (text + elements)
    wrapped = _wrap_xml_with_namespaces(xml_content)

    try:
        root = etree.fromstring(wrapped.encode("utf-8"))
    except etree.XMLSyntaxError:
        # Malformed XML — treat entire content as a single text element
        return [MessageElement.text(xml_content)]

    return _parse_children(root)


def _parse_children(parent: etree._Element) -> list[MessageElement]:
    """Recursively parse children of an lxml element into MessageElement list."""
    elements: list[MessageElement] = []

    # Leading text (text before first child element)
    if parent.text:
        text = parent.text
        if text:
            elements.append(MessageElement.text(text))

    for child in parent:
        tag = _normalize_tag(child.tag)
        attrs = _extract_attrs(child)

        # Recursively parse child elements for container tags
        children: list[MessageElement] = []
        if tag not in _VOID_TAGS:
            children = _parse_children(child)

        elements.append(MessageElement(type=tag, attrs=attrs, children=children))

        # Tail text (text after this element, before next sibling)
        if child.tail:
            tail = child.tail
            if tail:
                elements.append(MessageElement.text(tail))

    return elements


def _normalize_tag(tag: str) -> str:
    """Normalize lxml tag, handling potential namespace URIs.

    lxml represents namespaced tags as ``{uri}local``. We convert:
      - ``{urn:shinbot:elements}poke`` → ``sb:poke``
      - Any other ``{ns}tag`` → the raw ``ns:tag`` or just ``tag``
      - Plain local names pass through unchanged
    """
    if tag.startswith("{"):
        uri, _, local = tag[1:].partition("}")
        if uri == "urn:shinbot:elements":
            return f"sb:{local}"
        if uri.startswith(_SYNTHETIC_NS_PREFIX):
            prefix = uri.removeprefix(_SYNTHETIC_NS_PREFIX)
            return f"{prefix}:{local}" if prefix else local
        # For platform-specific namespaces (e.g. llonebot:ark),
        # try to preserve the prefix from the raw tag name
        return local
    # Handle colon-prefixed tags that lxml didn't namespace (common in Satori)
    return tag


def _extract_attrs(el: etree._Element) -> dict[str, str]:
    """Extract attributes from an lxml element, skipping xmlns declarations."""
    return {k: v for k, v in el.attrib.items() if not k.startswith("{") and k != "xmlns"}


def _wrap_xml_with_namespaces(xml_content: str) -> str:
    prefixes = _collect_tag_prefixes(xml_content)
    ns_attrs = [f'xmlns:{prefix}="{_namespace_uri_for_prefix(prefix)}"' for prefix in sorted(prefixes)]
    attr_text = f" {' '.join(ns_attrs)}" if ns_attrs else ""
    return f"<__root__{attr_text}>{xml_content}</__root__>"


def _collect_tag_prefixes(xml_content: str) -> set[str]:
    prefixes = {"sb"} if "sb:" in xml_content else set()
    for match in re.finditer(r"<\s*/?\s*([A-Za-z_][\w.-]*):[A-Za-z_][\w.-]*", xml_content):
        prefixes.add(match.group(1))
    return prefixes


def _namespace_uri_for_prefix(prefix: str) -> str:
    if prefix == "sb":
        return _NSMAP["sb"]
    return f"{_SYNTHETIC_NS_PREFIX}{prefix}"


# ── AST → XML ───────────────────────────────────────────────────────

# Characters requiring XML entity escaping in text content
_XML_ESCAPE_TABLE = str.maketrans(
    {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
    }
)

# Characters requiring XML entity escaping in attribute values
_ATTR_ESCAPE_TABLE = str.maketrans(
    {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
    }
)


def elements_to_xml(elements: Sequence[MessageElement]) -> str:
    """Serialize a list of MessageElement nodes back to Satori XML.

    Produces clean XML without an outer wrapper element.
    Text content and attribute values are properly entity-escaped.
    """
    parts: list[str] = []
    for el in elements:
        parts.append(_element_to_xml(el))
    return "".join(parts)


def _element_to_xml(el: MessageElement) -> str:
    """Serialize a single MessageElement to XML."""
    if el.type == "text":
        content = str(el.attrs.get("content", ""))
        return content.translate(_XML_ESCAPE_TABLE)

    if el.type == "br":
        return "<br/>"

    tag = el.type
    attr_str = _attrs_to_xml(el.attrs)

    if not el.children:
        # Self-closing tag
        if attr_str:
            return f"<{tag} {attr_str}/>"
        return f"<{tag}/>"

    # Container tag with children
    inner = elements_to_xml(el.children)
    if attr_str:
        return f"<{tag} {attr_str}>{inner}</{tag}>"
    return f"<{tag}>{inner}</{tag}>"


def _attrs_to_xml(attrs: dict[str, object]) -> str:
    """Serialize attribute dict to XML attribute string."""
    if not attrs:
        return ""
    parts: list[str] = []
    for key, value in attrs.items():
        escaped = str(value).translate(_ATTR_ESCAPE_TABLE)
        parts.append(f'{key}="{escaped}"')
    return " ".join(parts)
