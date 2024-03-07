#!/usr/bin/env bash

# This script builds the base image. It assumes that it is
# being built under GBI Lite. See the jobs in
# `ci/gitlab/stage-gbilite.yml`, as well as the scripts in
# `.campaigns`.

set -euo pipefail

# shellcheck source-path=SCRIPTDIR
# source "$(dirname "${BASH_SOURCE[0]}")/lib/dogweb_base_image.sh"

################################################################################

# GBILITE_IMAGE_TO_BUILD will be something like "dogweb-base-gbi:release";
# `repository` would thus be "dogweb-base-gbi".

# repository=$(cut -f1 -d: <<< "${GBILITE_IMAGE_TO_BUILD}")
repository="azure-log-forwarding-offering"
readonly repository

################################################################################

# version_tag="$(current_dogweb_version_tag)"
version_tag="v${BASE_PIPELINE_ID}-${CI_COMMIT_SHA:0:8}"
readonly version_tag

# Dump information about the generated image to a JSON file. This provides a lot
# of helpful information, but we particularly care about the digest of the image.
readonly metadata_file="image-metadata.json"

# We'll also tag this image with our traditional `v${PIPELINE_ID}-${COMMIT_SHA}`
# tag, even though most (all?) usage will be through the floating tag. The
# `image_info.txt` file generated at the end of this script also requires us to
# have a fixed tag (see comments there for more information).
# readonly floating_image_tag="registry.ddbuild.io/${GBILITE_IMAGE_TO_BUILD}"
readonly fixed_image_tag="registry.ddbuild.io/${repository}:${version_tag}"

# Just like the GBI Lite runs in DataDog/images, we explicitly opt-out of using
# a cache for building, to ensure that we always get a completely fresh and
# up-to-date build.
    # --tag "${floating_image_tag}" \
    # --metadata-file "${metadata_file}" \
    # --no-cache \


time docker buildx build \
    --platform="${PLATFORMS}" \
    --cache-to type=inline \
    --tag "${fixed_image_tag}" \
    --label git.repository="${CI_PROJECT_NAME}" \
    --label git.branch="${CI_COMMIT_REF_NAME}" \
    --label git.commit="${CI_COMMIT_SHA}" \
    --label ci.pipeline_id="${BASE_PIPELINE_ID}" \
    --label ci.job_id="${CI_JOB_ID}" \
    --label org.opencontainers.image.source="http://github.com/DataDog/${repository}" \
    --label org.opencontainers.image.vendor="datadog" \
    --label org.opencontainers.image.version="${version_tag}" \
    --label org.opencontainers.image.revision="${CI_COMMIT_SHA}" \
    --file "ci/docker/Dockerfile" \
    .

# Extract the digest from the metadata file for `ddsign`
# image_digest="$(jq --raw-output '.["containerimage.digest"]' "${metadata_file}")"
# readonly image_digest

# ddsign sign "${fixed_image_tag}@${image_digest}"

# Dump information out in a form that GBI Lite can consume.
#
# Note: the first line of the file should be the fixed version tag, not the
# floating one. GBI Lite automatically kicks off a Campaign to bump versions
# based on the contents of this file. In our case, that Campaign currently has
# nothing to do, because we don't *need* to bump versions, since this repository
# is the only consumer of the image, and we intentionally rely on the floating
# tag. As such, this file isn't actually used.
#
# However, if our image use logic should ever change for some reason, this file
# (properly constructed) will be able to support that change.

# cat << EOM > .campaigns/image_info.txt
# ${version_tag}
# ${image_digest}
# EOM

echo $fixed_image_tag > ci/image_tag.txt