#!/usr/bin/env bash

set -euxo pipefail

echo "[$CI_COMMIT_REF_NAME] - hello from pr-commenter" \
    | pr-commenter --header "Service pr-commenter test k8s" --for-pr $CI_COMMIT_REF_NAME
