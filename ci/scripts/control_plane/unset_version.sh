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

# Replace the provided version with "unknown"
sed -i '' "s/VERSION = \"$VERSION\"/VERSION = \"unknown\"/" "$VERSION_FILE"

# Verify the change
if grep -q "VERSION = \"unknown\"" "$VERSION_FILE"; then
    echo "Successfully reset version to unknown"
else
    echo "Error: Failed to reset version"
    exit 1
fi
