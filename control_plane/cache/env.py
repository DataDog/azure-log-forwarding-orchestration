# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from collections.abc import Callable
from logging import getLogger
from os import environ
from typing import TypeVar

T = TypeVar("T")

log = getLogger(__name__)


# Settings
STORAGE_CONNECTION_SETTING = "AzureWebJobsStorage"
DD_SITE_SETTING = "DD_SITE"
DD_API_KEY_SETTING = "DD_API_KEY"
DD_TELEMETRY_SETTING = "DD_TELEMETRY"
FORWARDER_IMAGE_SETTING = "FORWARDER_IMAGE"
SCALING_PERCENTAGE_SETTING = "SCALING_PERCENTAGE"
CONFIG_ID_SETTING = "CONFIG_ID"
SUBSCRIPTION_ID_SETTING = "SUBSCRIPTION_ID"
RESOURCE_GROUP_SETTING = "RESOURCE_GROUP"
CONTROL_PLANE_REGION_SETTING = "CONTROL_PLANE_REGION"
CONTROL_PLANE_ID_SETTING = "CONTROL_PLANE_ID"
MONITORED_SUBSCRIPTIONS_SETTING = "MONITORED_SUBSCRIPTIONS"
STORAGE_ACCOUNT_URL_SETTING = "STORAGE_ACCOUNT_URL"
PII_SCRUBBER_RULES_SETTING = "PII_SCRUBBER_RULES"
LOG_LEVEL_SETTING = "LOG_LEVEL"
RESOURCE_TAG_FILTERS_SETTING = "RESOURCE_TAG_FILTERS"
VERSION_TAG_SETTING = "VERSION_TAG"

# Secret Names
DD_API_KEY_SECRET = "dd-api-key"
CONNECTION_STRING_SECRET = "connection-string"


class MissingConfigOptionError(Exception):
    def __init__(self, option: str) -> None:
        super().__init__(f"Missing required configuration option: {option}")


def get_config_option(name: str) -> str:
    """Get a configuration option from the environment or raise a helpful error"""
    if option := environ.get(name):
        return option
    raise MissingConfigOptionError(name)


def parse_config_option(name: str, parse: Callable[[str], T | None], default: T) -> T:
    """Get a configuration option from the environment, parse it, or return a default"""
    try:
        value = environ.get(name)
        if value is None:
            return default
        result = parse(value)
        if result is None:
            log.error(f"Invalid value for configuration option {name}: {value}")
            return default
        return result
    except ValueError:
        log.error(f"Invalid value for configuration option {name}: {environ.get(name)}")
        return default


def is_truthy(setting_name: str) -> bool:
    return environ.get(setting_name, "").lower().strip() in {"t", "true", "1", "y", "yes"}
