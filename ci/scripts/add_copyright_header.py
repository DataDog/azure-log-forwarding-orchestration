#!/usr/bin/env python

from datetime import datetime
import os
import re
import sys
from typing import List, Optional, Dict


# List of folders to ignore
IGNORED_FOLDERS: List[str] = [
    "dist",
    "node_modules",
    "venv",
]

# File extensions and their comment styles
FILE_TYPES: Dict[str, str] = {
    ".py": "#",
    ".go": "//",
    ".sh": "#",
}


def read_template(template_path: str) -> str:
    """Read the template file and return its content."""
    try:
        with open(template_path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
        sys.exit(1)


def get_template_first_line(template_text: str, comment_char: str) -> str:
    """Get the first line of the template with the appropriate comment style."""
    lines: List[str] = template_text.splitlines()
    if not lines:
        return ""

    # Add comment prefix based on file type
    return f"{comment_char} {lines[0]}"


def create_header(template_text: str, comment_char: str) -> str:
    """Create a commented header from the template with the current year."""
    year = datetime.now().year

    # Replace $year with the actual year
    header_text: str = template_text.replace("$year", str(year))

    # Split into lines and add comment prefix based on file type
    commented_lines: List[str] = [f"{comment_char} {line}" for line in header_text.splitlines()]

    # Join with newlines and add an extra newline at the end
    return "\n".join(commented_lines) + "\n\n"


def has_header(content: str, template_first_line: str) -> bool:
    """Check if the content already has the header by looking for the first line."""
    return template_first_line in content


def check_and_update_files(directory: str, template_first_line: str, header: str, file_ext: str) -> List[str]:
    """Check files for the header and add it if missing."""
    modified_files: List[str] = []

    for root, dirs, files in os.walk(directory):
        # Skip ignored folders
        dirs[:] = [d for d in dirs if d not in IGNORED_FOLDERS]

        for file in files:
            if file.endswith(file_ext):
                file_path: str = os.path.join(root, file)
                try:
                    with open(file_path, "r") as f:
                        content: str = f.read()

                        # Check if the file already has the header
                        if not has_header(content, template_first_line):
                            # Check if the file starts with a shebang line
                            shebang_match: Optional[re.Match] = re.match(r"^#!.*\n", content)

                            if shebang_match:
                                # Insert header after shebang line
                                shebang_line: str = shebang_match.group(0)
                                rest_of_content: str = content[len(shebang_line) :]
                                new_content: str = shebang_line + header + rest_of_content
                            else:
                                # No shebang, add header at the beginning
                                new_content = header + content

                            # Write the updated content back to the file
                            with open(file_path, "w") as f:
                                f.write(new_content)
                            modified_files.append(file_path)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

    return modified_files


def main() -> int:
    # Get the script directory
    script_dir: str = os.path.dirname(os.path.abspath(__file__))

    # yell at me if these comments are still here
    # this file needs moved to licensing and then remove licensing here
    template_path: str = os.path.join(script_dir, "licensing", "template")

    # Read the template
    template_text: str = read_template(template_path)

    # Process each file type
    all_modified_files: List[str] = []

    for file_ext, comment_char in FILE_TYPES.items():
        # Get the first line of the template for header detection
        template_first_line: str = get_template_first_line(template_text, comment_char)

        # Create header with current year
        header: str = create_header(template_text, comment_char)

        # Check and update files
        modified_files: List[str] = check_and_update_files(".", template_first_line, header, file_ext)
        all_modified_files.extend(modified_files)

    # Print results
    if all_modified_files:
        print(f"Added copyright header to {len(all_modified_files)} files:")
        for file in all_modified_files:
            print(f"  - {file}")
        return 0
    else:
        print("All files already have the copyright header.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
