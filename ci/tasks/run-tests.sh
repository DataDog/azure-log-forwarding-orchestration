#!/usr/bin/env bash

set -euo pipefail

image_tag=$(cat ci/image_tag.txt)
readonly image_tag

time docker run --tag $image_tag