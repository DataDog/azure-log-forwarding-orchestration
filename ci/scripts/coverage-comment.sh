#!/usr/bin/env bash

set -euxo pipefail

curl -f -v 'https://pr-commenter.us1.ddbuild.io/internal/cit/pr-comment'\
    -H "$(/bin/authanywhere)"\
    -X PATCH \
    -d '{
    "commit": "'"$CI_COMMIT_REF_NAME"'",
    "message": $1" Coverage:\n'"$(cat $2)"'\n",
    "header": "Coverage Report",
    "org": "Datadog",
    "repo": "azure-log-forwarding-offering"
}'
