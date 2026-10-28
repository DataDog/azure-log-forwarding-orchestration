#!/usr/bin/env bash

set -uo pipefail
# The request expands an authorization header, so do not trace it.
set +x

curl -f -sS 'https://pr-commenter.us1.ddbuild.io/internal/cit/pr-comment'\
    -H "$(/bin/authanywhere)"\
    -X PATCH \
    -d '{
    "commit": "'"$CI_COMMIT_REF_NAME"'",
    "message": "Control Plane Coverage:\n'"$(awk -v ORS='\\n' '1' "ci/control_plane_coverage.md")"'\nForwarder Coverage:\n'"$(awk -v ORS='\\n' '1' "ci/forwarder_coverage.md")"'",
    "header": "Coverage Report",
    "org": "Datadog",
    "repo": "azure-log-forwarding-orchestration"
}'
