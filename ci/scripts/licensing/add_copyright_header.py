#!/usr/bin/env python
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

import fnmatch
import os
import re
import sys
from datetime import datetime


IGNORED_FOLDERS: list[str] = [
    ".git",
    "dist",
    "mocks",
    "node_modules",
    "venv",
]

# File extensions and their comment styles
FILE_TYPES: dict[str, str] = {
    ".py": "#",
    ".go": "//",
    ".sh": "#",
}


def read_gitignore(root_dir: str) -> set[str]:
    """Read .gitignore file and return a set of patterns to ignore."""
    gitignore_path = os.path.join(root_dir, ".gitignore")
    if not os.path.exists(gitignore_path):
        return set()

    patterns = set()
    try:
        with open(gitignore_path, "r") as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith("#"):
                    patterns.add(line)
    except Exception as e:
        print(f"Warning: Error reading .gitignore: {e}")

    return patterns


def should_ignore(path: str, gitignore_patterns: set[str]) -> bool:
    """Check if a path should be ignored based on .gitignore patterns."""
    # Convert path to relative path from the root
    rel_path = os.path.relpath(path, ".")

    # Check if the path matches any gitignore pattern
    for pattern in gitignore_patterns:
        if fnmatch.fnmatch(rel_path, pattern):
            return True
        # Also check if any parent directory matches
        parent = os.path.dirname(rel_path)
        while parent:
            if fnmatch.fnmatch(parent, pattern):
                return True
            parent = os.path.dirname(parent)

    return False


def read_template(template_path: str) -> str:
    """Read the template file and return its content."""
    try:
        with open(template_path) as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
        sys.exit(1)


def get_template_first_line(template_text: str, comment_char: str) -> str:
    """Get the first line of the template with the appropriate comment style."""
    lines = template_text.splitlines()
    if not lines:
        return ""

    return f"{comment_char} {lines[0]}"


def create_header(template_text: str, comment_char: str) -> str:
    """Create a commented header from the template with the current year."""
    year = datetime.now().year

    header_text = template_text.replace("$year", str(year))

    # Split into lines and add comment prefix based on file type
    commented_lines = [f"{comment_char} {line}" for line in header_text.splitlines()]

    return "\n".join(commented_lines) + "\n\n"


def has_header(content: str, template_first_line: str) -> bool:
    """Check if the content already has the header by looking for the first line."""
    return template_first_line in content


def check_and_update_files(
    directory: str, template_first_line: str, header: str, file_ext: str, gitignore_patterns: set[str]
) -> list[str]:
    """Check files for the header and add it if missing."""
    modified_files: list[str] = []

    for root, dirs, files in os.walk(directory):
        # Skip ignored folders and directories that match gitignore patterns
        dirs[:] = [
            d for d in dirs if d not in IGNORED_FOLDERS and not should_ignore(os.path.join(root, d), gitignore_patterns)
        ]

        for file in files:
            if not file.endswith(file_ext):
                continue
            file_path: str = os.path.join(root, file)

            # Skip files that match gitignore patterns
            if should_ignore(file_path, gitignore_patterns):
                continue

            try:
                with open(file_path) as f:
                    content: str = f.read()
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                sys.exit(1)

            if has_header(content, template_first_line):
                continue

            shebang_match: re.Match | None = re.match(r"^#!.*\n", content)

            if shebang_match:
                # Insert header after shebang line
                shebang_line: str = shebang_match.group(0)
                rest_of_content: str = content[len(shebang_line) :]
                new_content: str = shebang_line + header + rest_of_content
            else:
                # No shebang, add header at the beginning
                new_content = header + content

            with open(file_path, "w") as f:
                f.write(new_content)
            modified_files.append(file_path)

    return modified_files


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    template_path = os.path.join(script_dir, "template")

    template_text = read_template(template_path)

    gitignore_patterns = read_gitignore(".")

    all_modified_files: list[str] = []

    for file_ext, comment_char in FILE_TYPES.items():
        template_first_line = get_template_first_line(template_text, comment_char)

        header = create_header(template_text, comment_char)

        modified_files = check_and_update_files(".", template_first_line, header, file_ext, gitignore_patterns)
        all_modified_files.extend(modified_files)

    if all_modified_files:
        print(f"Added copyright header to {len(all_modified_files)} files:")
        for file in all_modified_files:
            print(f"  - {file}")
    else:
        print("All files already have the copyright header.")


if __name__ == "__main__":
    main()
