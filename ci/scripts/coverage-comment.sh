#!/usr/bin/env bash

set -euxo pipefail

pip install pycobertura

OUTPUT=$(pycobertura show --format markdown $2)

curl -f -v 'https://pr-commenter.us1.ddbuild.io/internal/cit/pr-comment'\
    -H "$(/bin/authanywhere)"\
    -X PATCH \
    -d '{
    "commit": "'"$CI_COMMIT_REF_NAME"'",
    "message": $1" Coverage:\n'"$OUTPUT"'\n",
    "header": "Coverage Report",
    "org": "Datadog",
    "repo": "azure-log-forwarding-offering"
}'
