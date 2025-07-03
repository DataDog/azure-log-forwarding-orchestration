# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from os import environ

# project
from cache.env import DD_API_KEY_SETTING, DD_TELEMETRY_SETTING, is_truthy

TELEMETRY_ENABLED = bool(is_truthy(DD_TELEMETRY_SETTING) and environ.get(DD_API_KEY_SETTING))

if TELEMETRY_ENABLED:
    import ddtrace.auto  # noqa: F401
    from datadog_serverless_compat import start
    from ddtrace.runtime import RuntimeMetrics

    start()
    RuntimeMetrics.enable()
