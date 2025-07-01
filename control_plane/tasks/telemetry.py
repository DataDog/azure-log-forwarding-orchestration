from os import environ

from cache.env import DD_API_KEY_SETTING, DD_TELEMETRY_SETTING, is_truthy

TELEMETRY_ENABLED = bool(is_truthy(DD_TELEMETRY_SETTING) and environ.get(DD_API_KEY_SETTING))

if TELEMETRY_ENABLED:
    import ddtrace.auto  # noqa: F401
    from datadog import initialize
    from datadog_serverless_compat import start
    from ddtrace.runtime import RuntimeMetrics

    start()
    initialize()
    RuntimeMetrics.enable()
