#!/usr/bin/env bash

set -euo pipefail

command -v check-jsonschema > /dev/null || pip install check-jsonschema

# get schema
schemafile=$(python -c "import json; schema=json.load(open('$1')).get('\$schema'); schema and print(schema, end='')")
if [ -z "$schemafile" ]; then
  echo "No schema found in $1"
  exit
fi

check-jsonschema --schemafile=$schemafile $1
