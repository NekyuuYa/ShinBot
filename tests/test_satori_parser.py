"""Tests for shinbot.utils.satori_parser — XML ⇌ AST serialization."""

import pytest

from shinbot.models.elements import Message, MessageElement
from shinbot.utils.satori_parser import elements_to_xml, parse_xml


class TestParseXML:
    """Test XML → AST parsing."""

    def test_empty_string(self):
        assert parse_xml("") == []

    def test_whitespace_only(self):
        assert parse_xml("   ") == []

    def test_plain_text(self):
        result = parse_xml("hello world")
        assert len(result) == 1
        assert result[0].type == "text"
        assert result[0].text_content == "hello world"

    def test_single_self_closing_tag(self):
        result = parse_xml('<at id="123"/>')
        assert len(result) == 1
        assert result[0].type == "at"
        assert result[0].attrs["id"] == "123"

    def test_text_with_tag(self):
        result = parse_xml('hello <at id="123"/> world')
        assert len(result) == 3
        assert result[0].type == "text"
        assert result[0].text_content == "hello "
        assert result[1].type == "at"
        assert result[1].attrs["id"] == "123"
        assert result[2].type == "text"
        assert result[2].text_content == " world"

    def test_img_tag_with_attrs(self):
        xml = '<img width="800" height="600" src="https://example.com/a.png"/>'
        result = parse_xml(xml)
        assert len(result) == 1
        assert result[0].type == "img"
        assert result[0].attrs["src"] == "https://example.com/a.png"
        assert result[0].attrs["width"] == "800"

    def test_quote_with_nested_content(self):
        xml = '<quote id="msg-1">quoted text<img src="x.png"/></quote>'
        result = parse_xml(xml)
        assert len(result) == 1
        quote = result[0]
        assert quote.type == "quote"
        assert quote.attrs["id"] == "msg-1"
        assert len(quote.children) == 2
        assert quote.children[0].type == "text"
        assert quote.children[0].text_content == "quoted text"
        assert quote.children[1].type == "img"

    def test_br_tag(self):
        result = parse_xml("line1<br/>line2")
        assert len(result) == 3
        assert result[0].text_content == "line1"
        assert result[1].type == "br"
        assert result[2].text_content == "line2"

    def test_entity_escaping(self):
        """Entities like &amp; should be properly decoded."""
        result = parse_xml("A &amp; B &lt; C")
        assert len(result) == 1
        assert result[0].text_content == "A & B < C"

    def test_entity_in_attr(self):
        xml = '<img src="https://example.com?a=1&amp;b=2"/>'
        result = parse_xml(xml)
        assert result[0].attrs["src"] == "https://example.com?a=1&b=2"

    def test_multiple_elements(self):
        xml = '<at id="1"/><at id="2"/><at id="3"/>'
        result = parse_xml(xml)
        assert len(result) == 3
        assert all(el.type == "at" for el in result)

    def test_platform_specific_tag(self):
        """Platform-specific tags (e.g. llonebot:ark) should be preserved."""
        xml = '<llonebot:ark data="{}"/>'
        result = parse_xml(xml)
        assert len(result) == 1
        assert result[0].type == "llonebot:ark"

    def test_sb_namespace_extension_tag(self):
        xml = '<message forward="true"><sb:ark data="{&quot;x&quot;:1}"/></message>'
        result = parse_xml(xml)
        assert len(result) == 1
        assert result[0].type == "message"
        assert len(result[0].children) == 1
        assert result[0].children[0].type == "sb:ark"

    def test_multiple_prefixed_tags_parse_without_text_fallback(self):
        xml = '<qq:markdown content="**hi**"/><sb:poke target="42" type="poke"/>'
        result = parse_xml(xml)
        assert [el.type for el in result] == ["qq:markdown", "sb:poke"]

    def test_malformed_xml_fallback(self):
        """Malformed XML should fall back to a single text element."""
        result = parse_xml("<broken attr= value>")
        assert len(result) == 1
        assert result[0].type == "text"

    def test_real_satori_text_message(self):
        """Parse a real Satori message content — pure text."""
        result = parse_xml("噼里啪啦噼里啪啦")
        assert len(result) == 1
        assert result[0].text_content == "噼里啪啦噼里啪啦"

    def test_real_satori_mixed_message(self):
        """Parse a real Satori message with text + img."""
        xml = '色情图片<img width="878" height="1920" sub-type="0" src="https://example.com/img.png"/>'
        result = parse_xml(xml)
        assert len(result) == 2
        assert result[0].type == "text"
        assert result[0].text_content == "色情图片"
        assert result[1].type == "img"
        assert result[1].attrs["src"] == "https://example.com/img.png"
        assert result[1].attrs["sub-type"] == "0"

    def test_real_satori_quote_message(self):
        """Parse a real Satori message with quote + text + img."""
        xml = (
            '<quote id="7627595803300628331">'
            "图片混合测试"
            '<img width="3072" height="4096" sub-type="0" src="https://example.com/img.png"/>'
            "</quote>"
            "这是一条引用"
        )
        result = parse_xml(xml)
        assert len(result) == 2
        quote = result[0]
        assert quote.type == "quote"
        assert quote.attrs["id"] == "7627595803300628331"
        assert len(quote.children) == 2
        assert quote.children[0].text_content == "图片混合测试"
        assert quote.children[1].type == "img"
        assert result[1].text_content == "这是一条引用"


class TestElementsToXML:
    """Test AST → XML serialization."""

    def test_text_element(self):
        xml = elements_to_xml([MessageElement.text("hello")])
        assert xml == "hello"

    def test_text_entity_escape(self):
        xml = elements_to_xml([MessageElement.text("A & B < C > D")])
        assert xml == "A &amp; B &lt; C &gt; D"

    def test_self_closing_tag(self):
        xml = elements_to_xml([MessageElement.at(id="123")])
        assert xml == '<at id="123"/>'

    def test_br_tag(self):
        xml = elements_to_xml([MessageElement.br()])
        assert xml == "<br/>"

    def test_img_with_attrs(self):
        xml = elements_to_xml([MessageElement.img("https://example.com/a.png", width="100")])
        assert 'src="https://example.com/a.png"' in xml
        assert 'width="100"' in xml
        assert xml.endswith("/>")

    def test_quote_with_children(self):
        children = [MessageElement.text("hello"), MessageElement.img("x.png")]
        el = MessageElement.quote("msg-1", children=children)
        xml = elements_to_xml([el])
        assert xml == '<quote id="msg-1">hello<img src="x.png"/></quote>'

    def test_attr_entity_escape(self):
        el = MessageElement.img("https://example.com?a=1&b=2")
        xml = elements_to_xml([el])
        assert "&amp;" in xml
        assert "&quot;" not in xml  # no quotes inside attr values here

    def test_mixed_elements(self):
        elements = [
            MessageElement.text("hello "),
            MessageElement.at(id="123"),
            MessageElement.text(" world"),
        ]
        xml = elements_to_xml(elements)
        assert xml == 'hello <at id="123"/> world'

    def test_empty_list(self):
        assert elements_to_xml([]) == ""

    def test_extension_tag(self):
        el = MessageElement(type="sb:poke", attrs={"target": "123", "type": "shake"})
        xml = elements_to_xml([el])
        assert "<sb:poke" in xml
        assert 'target="123"' in xml


class TestRoundTrip:
    """Test XML → AST → XML round-trip fidelity."""

    @pytest.mark.parametrize(
        "xml_input",
        [
            "hello world",
            '<at id="123"/>',
            'hello <at id="123" name="Alice"/> world',
            "<br/>",
            "line1<br/>line2",
            '<img src="https://example.com/a.png" width="100"/>',
            '<quote id="msg-1">quoted text</quote>reply',
        ],
    )
    def test_roundtrip(self, xml_input: str):
        """Parsing then serializing should produce equivalent XML."""
        elements = parse_xml(xml_input)
        output = elements_to_xml(elements)
        assert output == xml_input

    def test_entity_roundtrip(self):
        """Entities should survive round-trip."""
        original = "A &amp; B &lt; C"
        elements = parse_xml(original)
        # After parsing, text content should be decoded
        assert elements[0].text_content == "A & B < C"
        # After re-serializing, entities should be re-encoded
        output = elements_to_xml(elements)
        assert output == original


class TestMessageFromXML:
    """Test Message.from_xml() integration."""

    def test_from_xml(self):
        msg = Message.from_xml('hello <at id="123"/> world')
        assert len(msg) == 3
        assert msg.get_text() == "hello  world"

    def test_to_xml(self):
        msg = Message.from_elements(
            MessageElement.text("hi "),
            MessageElement.at(id="456"),
        )
        xml = msg.to_xml()
        assert xml == 'hi <at id="456"/>'

    def test_from_xml_to_xml_roundtrip(self):
        original = '<quote id="1">text<img src="x.png"/></quote>reply'
        msg = Message.from_xml(original)
        assert msg.to_xml() == original
