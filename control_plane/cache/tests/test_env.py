# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/  Copyright 2025 Datadog, Inc.

from unittest import TestCase
from unittest.mock import patch

from cache.env import SCALING_PERCENTAGE_SETTING, MissingConfigOptionError, get_config_option, parse_config_option


class TestGetConfigOption(TestCase):
    def test_missing_config_option(self):
        with self.assertRaises(MissingConfigOptionError) as ctx:
            get_config_option("missing_option")

        self.assertEqual(str(ctx.exception), "Missing required configuration option: missing_option")

    @patch.dict("cache.env.environ", {"DD_SITE": "datadog_site", "DD_API_KEY": "datadog_api"})
    def test_get_config_option(self):
        self.assertEqual(get_config_option("DD_SITE"), "datadog_site")
        self.assertEqual(get_config_option("DD_API_KEY"), "datadog_api")


class TestParseConfigOption(TestCase):
    @patch.dict("cache.env.environ", {SCALING_PERCENTAGE_SETTING: "50"})
    def test_parse_config_option_valid(self):
        result = parse_config_option(SCALING_PERCENTAGE_SETTING, int, 100)
        self.assertEqual(result, 50)

    @patch.dict("cache.env.environ", {SCALING_PERCENTAGE_SETTING: "invalid"})
    def test_parse_config_option_invalid(self):
        result = parse_config_option(SCALING_PERCENTAGE_SETTING, int, 100)
        self.assertEqual(result, 100)

    @patch.dict("cache.env.environ", {})
    def test_parse_config_option_missing(self):
        result = parse_config_option(SCALING_PERCENTAGE_SETTING, int, 100)
        self.assertEqual(result, 100)

    @patch.dict("cache.env.environ", {SCALING_PERCENTAGE_SETTING: "hi"})
    def test_parse_config_option_parser_returns_none(self):
        result = parse_config_option(SCALING_PERCENTAGE_SETTING, lambda _: None, 100)
        self.assertEqual(result, 100)
