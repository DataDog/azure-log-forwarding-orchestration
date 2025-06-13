# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from json import dumps
from logging import Logger

# 3p
from yaml import YAMLError, safe_load


def convert_pii_rules_to_json(pii_scrubber_rules_yaml: str, log: Logger) -> str:
    parsed_yaml = None
    pii_rules_json = "{}"
    try:
        parsed_yaml = safe_load(pii_scrubber_rules_yaml)
    except YAMLError:
        log.exception("Error parsing PII scrubber rules as YAML:\n%s", pii_scrubber_rules_yaml)
        return pii_rules_json

    if not parsed_yaml:
        return pii_rules_json

    try:
        return dumps(parsed_yaml)
    except Exception:
        log.exception("Error converting PII scrubber rules to JSON:\n%s", pii_scrubber_rules_yaml)
        return pii_rules_json
