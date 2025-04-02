#!/usr/bin/env python

import os
import re
import sys

# List of folders to ignore
IGNORED_FOLDERS = [
    "dist",
    "node_modules",
    "venv",
]


def read_template(template_path):
    """Read the template file and return its content."""
    try:
        with open(template_path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
        sys.exit(1)


def create_regex_pattern(template_text):
    """Create a regex pattern from the template, replacing $year with a regex for years."""
    # Split the template into lines and add comment prefix to each line
    commented_lines = [f"# {line}" for line in template_text.splitlines()]
    commented_template = "\n".join(commented_lines)

    # Replace $year with a regex pattern that matches 4-digit years
    pattern = commented_template.replace("$year", r"\d{4}")

    # Escape special regex characters
    pattern = re.escape(pattern)

    # Restore the year pattern
    pattern = pattern.replace(r"\d\{4\}", r"\d{4}")

    return re.compile(pattern, re.MULTILINE | re.DOTALL)


def check_python_files(directory, pattern):
    """Check all Python files in the directory and subdirectories for the header."""
    missing_header_files = []

    for root, dirs, files in os.walk(directory):
        # Skip ignored folders
        dirs[:] = [d for d in dirs if d not in IGNORED_FOLDERS]

        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, "r") as f:
                        content = f.read()
                        if not pattern.search(content):
                            missing_header_files.append(file_path)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    return missing_header_files


def main():
    # Get the script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(script_dir, "licensing", "template")

    # Read the template
    template_text = read_template(template_path)

    # Create regex pattern
    pattern = create_regex_pattern(template_text)

    # Check Python files
    missing_header_files = check_python_files(".", pattern)

    # Print results
    if missing_header_files:
        print("The following Python files are missing the copyright header:")
        for file in missing_header_files:
            print(f"  - {file}")
        return 1
    else:
        print("All Python files have the copyright header.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
