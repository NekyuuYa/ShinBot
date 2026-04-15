"""Tests for message element schema."""

from shinbot.schema.elements import Message, MessageElement


class TestMessageElement:
    """Test MessageElement construction and properties."""

    def test_text_factory(self):
        el = MessageElement.text("hello")
        assert el.type == "text"
        assert el.attrs == {"content": "hello"}
        assert el.children == []

    def test_at_factory_with_id(self):
        el = MessageElement.at(id="12345")
        assert el.type == "at"
        assert el.attrs == {"id": "12345"}

    def test_at_factory_with_type_all(self):
        el = MessageElement.at(type="all")
        assert el.type == "at"
        assert el.attrs == {"type": "all"}

    def test_at_factory_with_all_fields(self):
        el = MessageElement.at(id="123", name="Alice", type="here")
        assert el.attrs == {"id": "123", "name": "Alice", "type": "here"}

    def test_sharp_factory(self):
        el = MessageElement.sharp(id="chan-1", name="general")
        assert el.type == "sharp"
        assert el.attrs == {"id": "chan-1", "name": "general"}

    def test_img_factory(self):
        el = MessageElement.img("https://example.com/a.png", width="100", height="200")
        assert el.type == "img"
        assert el.attrs["src"] == "https://example.com/a.png"
        assert el.attrs["width"] == "100"

    def test_emoji_factory(self):
        el = MessageElement.emoji(id="face_1", name="smile")
        assert el.type == "emoji"
        assert el.attrs == {"id": "face_1", "name": "smile"}

    def test_quote_factory_with_children(self):
        child = MessageElement.text("quoted text")
        el = MessageElement.quote("msg-1", children=[child])
        assert el.type == "quote"
        assert el.attrs == {"id": "msg-1"}
        assert len(el.children) == 1
        assert el.children[0].text_content == "quoted text"

    def test_audio_factory(self):
        el = MessageElement.audio("https://example.com/a.mp3", duration="30")
        assert el.type == "audio"
        assert el.attrs["src"] == "https://example.com/a.mp3"
        assert el.attrs["duration"] == "30"

    def test_video_factory(self):
        el = MessageElement.video("https://example.com/v.mp4")
        assert el.type == "video"
        assert el.attrs["src"] == "https://example.com/v.mp4"

    def test_file_factory(self):
        el = MessageElement.file("path/to/file.zip", title="backup", size="1024")
        assert el.type == "file"
        assert el.attrs["title"] == "backup"

    def test_br_factory(self):
        el = MessageElement.br()
        assert el.type == "br"
        assert el.attrs == {}
        assert el.children == []

    def test_is_text_property(self):
        assert MessageElement.text("hi").is_text is True
        assert MessageElement.br().is_text is False
        assert MessageElement.at(id="1").is_text is False

    def test_text_content_property(self):
        assert MessageElement.text("hello").text_content == "hello"
        assert MessageElement.br().text_content == ""
        assert MessageElement.at(id="1").text_content == ""

    def test_generic_construction(self):
        el = MessageElement(type="sb:poke", attrs={"target": "123", "type": "shake"})
        assert el.type == "sb:poke"
        assert el.attrs["target"] == "123"

    def test_repr(self):
        el = MessageElement.text("hi")
        assert "text" in repr(el)


class TestMessage:
    """Test Message container."""

    def test_from_text(self):
        msg = Message.from_text("hello world")
        assert len(msg) == 1
        assert msg[0].type == "text"
        assert msg[0].text_content == "hello world"

    def test_from_elements(self):
        msg = Message.from_elements(
            MessageElement.text("hi "),
            MessageElement.at(id="123"),
            MessageElement.text(" there"),
        )
        assert len(msg) == 3
        assert msg[0].is_text
        assert msg[1].type == "at"

    def test_get_text_simple(self):
        msg = Message.from_text("hello")
        assert msg.get_text() == "hello"
        assert msg.text == "hello"

    def test_get_text_mixed(self):
        msg = Message.from_elements(
            MessageElement.text("hello "),
            MessageElement.at(id="123"),
            MessageElement.text(" world"),
        )
        assert msg.get_text() == "hello  world"

    def test_get_text_with_br(self):
        msg = Message.from_elements(
            MessageElement.text("line1"),
            MessageElement.br(),
            MessageElement.text("line2"),
        )
        assert msg.get_text() == "line1\nline2"

    def test_get_text_recursive(self):
        """Text extraction should descend into children (e.g. quote)."""
        inner = MessageElement.text("quoted")
        quote = MessageElement.quote("1", children=[inner])
        msg = Message.from_elements(
            MessageElement.text("reply: "),
            quote,
        )
        assert msg.get_text() == "reply: quoted"

    def test_len_and_getitem(self):
        msg = Message.from_elements(
            MessageElement.text("a"),
            MessageElement.text("b"),
        )
        assert len(msg) == 2
        assert msg[0].text_content == "a"
        assert msg[1].text_content == "b"

    def test_iter(self):
        msg = Message.from_elements(
            MessageElement.text("x"),
            MessageElement.text("y"),
        )
        texts = [el.text_content for el in msg]
        assert texts == ["x", "y"]

    def test_bool_empty(self):
        assert not Message()
        assert Message.from_text("hi")

    def test_append(self):
        msg = Message.from_text("start")
        msg.append(MessageElement.text(" end"))
        assert len(msg) == 2
        assert msg.get_text() == "start end"

    def test_extend(self):
        msg = Message.from_text("start")
        msg.extend([MessageElement.text(" mid"), MessageElement.text(" end")])
        assert len(msg) == 3

    def test_add(self):
        a = Message.from_text("hello ")
        b = Message.from_text("world")
        combined = a + b
        assert len(combined) == 2
        assert combined.get_text() == "hello world"

    def test_repr(self):
        msg = Message.from_text("hi")
        assert "1" in repr(msg)

    def test_empty_message(self):
        msg = Message()
        assert len(msg) == 0
        assert msg.get_text() == ""
