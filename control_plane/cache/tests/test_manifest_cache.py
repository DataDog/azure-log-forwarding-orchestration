# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/  Copyright 2025 Datadog, Inc.

# stdlib
from json import dumps
from unittest import TestCase

from cache.manifest_cache import deserialize_manifest_cache


class TestManifestCache(TestCase):
    def test_validate_manifest_cache_normal(self):
        original_cache = {
            "resources": "08619d70937787adcea83e7d0b739f7f56ab932e07c8874c9894eec26a7417f3",
            "diagnostic_settings": "7e4765d9c184a79b2916aca3a80ae4110cd0d51486d64b9904cfaa6b44a950c8",
            "scaling": "de03f63c0058dc96964e426b1590eaf8234de9e52280c27bfeee496d1b2d342f",
        }
        manifest_cache_str = dumps(original_cache)
        self.assertEqual(original_cache, deserialize_manifest_cache(manifest_cache_str))

    def test_validate_manifest_cache_numbers(self):
        original_cache = {
            "resources": "08619d70937787adcea83e7d0b739f7f56ab932e07c8874c9894eec26a7417f3",
            "diagnostic_settings": 3452678932789,
            "scaling": "de03f63c0058dc96964e426b1590eaf8234de9e52280c27bfeee496d1b2d342f",
        }
        manifest_cache_str = dumps(original_cache)
        self.assertIsNone(deserialize_manifest_cache(manifest_cache_str))

    def test_validate_manifest_cache_missing_entry(self):
        original_cache = {
            "resources": "08619d70937787adcea83e7d0b739f7f56ab932e07c8874c9894eec26a7417f3",
            "diagnostic_settings": "7e4765d9c184a79b2916aca3a80ae4110cd0d51486d64b9904cfaa6b44a950c8",
        }
        manifest_cache_str = dumps(original_cache)
        self.assertIsNone(deserialize_manifest_cache(manifest_cache_str))

    def test_prune_cache(self):
        original_cache = {
            "resources": "08619d70937787adcea83e7d0b739f7f56ab932e07c8874c9894eec26a7417f3",
            "diagnostic_settings": "7e4765d9c184a79b2916aca3a80ae4110cd0d51486d64b9904cfaa6b44a950c8",
            "scaling": "de03f63c0058dc96964e426b1590eaf8234de9e52280c27bfeee496d1b2d342f",
            "something_else": "90eaf8234de9e52280c27bfeee496d1b2d342fde03f63c0058dc96964e426b15",
        }
        manifest_cache_str = dumps(original_cache)
        cache = deserialize_manifest_cache(manifest_cache_str)
        self.assertEqual(
            {
                "resources": "08619d70937787adcea83e7d0b739f7f56ab932e07c8874c9894eec26a7417f3",
                "diagnostic_settings": "7e4765d9c184a79b2916aca3a80ae4110cd0d51486d64b9904cfaa6b44a950c8",
                "scaling": "de03f63c0058dc96964e426b1590eaf8234de9e52280c27bfeee496d1b2d342f",
            },
            cache,
        )
