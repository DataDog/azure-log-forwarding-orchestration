#!/usr/bin/env bash

set -euo pipefail

if [ "${CI:-}" == 'true' ]; then
  pip install check-jsonschema
  schemafile=$(python -c "import json; schema=json.load(open('$1')).get('\$schema'); schema and print(schema, end='')")
else
  schemafile=$(jq -r '.["$schema"]' deploy/*.json)
fi

# get schema
if [ -z "$schemafile" ]; then
  echo "No schema found in $1"
  exit
fi

python -m check_jsonschema --schemafile=$schemafile $1
