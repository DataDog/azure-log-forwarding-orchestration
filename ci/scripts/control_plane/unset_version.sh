#!/bin/bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.


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
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/VERSION = \"$VERSION\"/VERSION = \"unknown\"/" "$VERSION_FILE"
else
    sed -i "s/VERSION = \"$VERSION\"/VERSION = \"unknown\"/" "$VERSION_FILE"
fi

# Verify the change
if grep -q "VERSION = \"unknown\"" "$VERSION_FILE"; then
    echo "Successfully reset version to unknown"
else
    echo "Error: Failed to reset version"
    exit 1
fi
