from unittest import TestCase

from cache.env import MissingConfigOptionError, get_config_option


class TestCommon(TestCase):
    def test_missing_config_option(self):
        with self.assertRaises(MissingConfigOptionError) as ctx:
            get_config_option("missing_option")

        self.assertEqual(str(ctx.exception), "Missing required configuration option: missing_option")
