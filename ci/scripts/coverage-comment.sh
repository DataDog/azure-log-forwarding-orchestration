#!/usr/bin/env bash

set -uxo pipefail

curl -f -v 'https://pr-commenter.us1.ddbuild.io/internal/cit/pr-comment'\
    -H "$(/bin/authanywhere)"\
    -X PATCH \
    -d '{
    "commit": "'"$CI_COMMIT_REF_NAME"'",
    "message": "Control Plane Coverage:\n'"$(awk -v ORS='\\n' '1' "ci/control_plane_coverage.md")"'\nForwarder Coverage:\n'"$(awk -v ORS='\\n' '1' "ci/forwarder_coverage.md")"'",
    "header": "Coverage Report",
    "org": "Datadog",
    "repo": "azure-log-forwarding-orchestration"
}'
