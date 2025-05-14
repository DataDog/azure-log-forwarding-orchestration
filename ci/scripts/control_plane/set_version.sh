#!/bin/bash

# Check if version argument is provided
if [ $# -eq 0 ]; then
    echo "Error: Version argument is required"
    echo "Usage: $0 <version>"
    exit 1
fi

VERSION=$1
VERSION_FILE="control_plane/tasks/version.py"

# Check if version file exists
if [ ! -f "$VERSION_FILE" ]; then
    echo "Error: Version file not found at $VERSION_FILE"
    exit 1
fi

# Replace "unset" with the provided version
sed -i '' "s/VERSION = \"unset\"/VERSION = \"$VERSION\"/" "$VERSION_FILE"

# Verify the change
if grep -q "VERSION = \"$VERSION\"" "$VERSION_FILE"; then
    echo "Successfully updated version to $VERSION"
else
    echo "Error: Failed to update version"
    exit 1
fi
