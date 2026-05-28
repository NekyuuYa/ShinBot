"""Plugin scaffold generator for ShinBot."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

_PLUGIN_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _validate_name(name: str) -> str:
    """Validate and normalize a plugin name."""
    name = name.strip()
    if not name:
        print("Error: plugin name cannot be empty.", file=sys.stderr)
        sys.exit(1)
    if not _PLUGIN_NAME_RE.match(name):
        print(
            f"Error: invalid plugin name {name!r}. "
            "Use lowercase letters, digits, and underscores (must start with a letter).",
            file=sys.stderr,
        )
        sys.exit(1)
    return name


def _plugin_dir_name(name: str) -> str:
    """Derive the directory name: shinbot_plugin_{name}."""
    if name.startswith("shinbot_plugin_") or name.startswith("shinbot_adapter_"):
        return name
    return f"shinbot_plugin_{name}"


def _module_id(dir_name: str) -> str:
    """Derive the module ID from the directory name."""
    return dir_name


def generate_plugin_scaffold(
    name: str,
    output_dir: Path | None = None,
) -> Path:
    """Generate a plugin scaffold directory.

    Returns the path to the created plugin directory.
    """
    name = _validate_name(name)
    dir_name = _plugin_dir_name(name)
    module_id = _module_id(dir_name)

    if output_dir is None:
        output_dir = Path(".")

    plugin_path = output_dir / dir_name
    if plugin_path.exists():
        print(f"Error: directory {plugin_path} already exists.", file=sys.stderr)
        sys.exit(1)

    plugin_path.mkdir(parents=True)

    # metadata.json
    metadata = {
        "id": module_id,
        "name": name.replace("_", " ").title(),
        "version": "0.1.0",
        "author": "",
        "description": "",
        "entry": "__init__.py",
        "role": "logic",
        "permissions": [],
    }
    (plugin_path / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )

    # __init__.py
    (plugin_path / "__init__.py").write_text(
        f'"""Plugin: {name}."""\n'
        "\n"
        "from __future__ import annotations\n"
        "\n"
        "from shinbot.core.plugins.context import Plugin\n"
        "\n"
        "\n"
        f"def setup(plg: Plugin) -> None:\n"
        f'    """Register the {name} plugin."""\n'
        "    # TODO: implement plugin logic\n"
        "    pass\n",
        encoding="utf-8",
    )

    # config.example.toml
    (plugin_path / "config.example.toml").write_text(
        "# Plugin configuration\n"
        "\n",
        encoding="utf-8",
    )

    # config.schema.toml
    (plugin_path / "config.schema.toml").write_text(
        "[provider]\n"
        'kind = "plugin"\n'
        f'id = "{module_id}"\n'
        f'display_name = "{module_id}"\n'
        'description = ""\n'
        'config_version = "1.0.0"\n',
        encoding="utf-8",
    )

    return plugin_path


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for shinbot create-plugin."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="shinbot create-plugin",
        description="Generate a new ShinBot plugin scaffold.",
    )
    parser.add_argument(
        "name",
        help="Plugin name (e.g. 'my_tool' creates shinbot_plugin_my_tool/)",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=".",
        metavar="DIR",
        help="Parent directory for the plugin (default: current directory)",
    )
    args = parser.parse_args(argv)

    path = generate_plugin_scaffold(args.name, Path(args.output_dir))
    print(f"Plugin scaffold created at: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
