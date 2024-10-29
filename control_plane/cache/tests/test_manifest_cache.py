# stdlib
from json import dumps
from unittest import TestCase

from cache.manifest_cache import PublicManifest, deserialize_manifest_cache, deserialize_public_manifest


class TestManifestCache(TestCase):
    def test_validate_manifest_cache_normal(self):
        original_cache = {
            "resources": "v47826247-f7abf93e",
            "diagnostic_settings": "v47826247-f7abf93e",
            "scaling": "v47826247-f7abf93e",
        }
        manifest_cache_str = dumps(original_cache)
        self.assertEqual(original_cache, deserialize_manifest_cache(manifest_cache_str))

    def test_validate_manifest_cache_numbers(self):
        original_cache = {
            "resources": 3452678932789,
            "diagnostic_settings": "v47826247-f7abf93e",
            "scaling": "v47826247-f7abf93e",
        }
        manifest_cache_str = dumps(original_cache)
        self.assertIsNone(deserialize_manifest_cache(manifest_cache_str))

    def test_validate_manifest_invalid_version_tag(self):
        original_cache = {
            "resources": "v47826a47-f7abf93e",  # first part cannot have letters (even hexadecimal)
            "diagnostic_settings": "v47826247-f7abf93e",
            "scaling": "v47826247-f7abf93e",
        }
        manifest_cache_str = dumps(original_cache)
        self.assertIsNone(deserialize_manifest_cache(manifest_cache_str))

        original_cache = {
            "resources": "v47826247-f7abf93e",
            "diagnostic_settings": "47826247-f7abf9ef",  # needs to start with `v`
            "scaling": "v47826247-f7abf93e",
        }
        manifest_cache_str = dumps(original_cache)
        self.assertIsNone(deserialize_manifest_cache(manifest_cache_str))
        original_cache = {
            "resources": "v47826247-f7abf93e",
            "diagnostic_settings": "v4782624700-f7abf9",
            "scaling": "v47826247-f7abf93x",  # can only contain hex characters for the git sha
        }
        manifest_cache_str = dumps(original_cache)
        self.assertIsNone(deserialize_manifest_cache(manifest_cache_str))

    def test_validate_manifest_cache_additional_entry(self):
        original_cache = {
            "resources": "v47826247-f7abf93e",
            "diagnostic_settings": "v47826247-f7abf93e",
            "scaling": "v47826247-f7abf93e",
            "extra": "v47826247-f7abf93e",
        }
        manifest_cache_str = dumps(original_cache)
        self.assertIsNone(deserialize_manifest_cache(manifest_cache_str))

    def test_partial_cache_valid(self):
        original_cache = {"resources": "v47826247-f7abf93e"}
        manifest_cache_str = dumps(original_cache)
        self.assertEqual(original_cache, deserialize_manifest_cache(manifest_cache_str))

        original_cache = {"diagnostic_settings": "v47826247-f7abf93e"}
        manifest_cache_str = dumps(original_cache)
        self.assertEqual(original_cache, deserialize_manifest_cache(manifest_cache_str))

        original_cache = {"scaling": "v47826247-f7abf93e"}
        manifest_cache_str = dumps(original_cache)
        self.assertEqual(original_cache, deserialize_manifest_cache(manifest_cache_str))


class TestPublicManifest(TestCase):
    def setUp(self) -> None:
        self.manifest: PublicManifest = {
            "latest": {
                "resources": "v47826247-f7abf93e",
                "diagnostic_settings": "v47826247-f7abf93e",
                "scaling": "v47826247-f7abf93e",
            },
            "version_history": {
                "v47826247-f7abf93e": {
                    "resources": "19408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "diagnostic_settings": "29408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "scaling": "39408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                },
                "v47707852-0fafeed8": {
                    "resources": "49408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "diagnostic_settings": "59408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "scaling": "69408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                },
                "v47589963-6b32e37e": {
                    "resources": "79408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "diagnostic_settings": "89408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                    "scaling": "99408c5738ad9d09e70f45510ce770979552d9bcf88d873e2ea17d705639635d",
                },
            },
        }

    def test_validate_public_manifest_normal(self):
        manifest_str = dumps(self.manifest)
        self.assertEqual(self.manifest, deserialize_public_manifest(manifest_str))

    def test_validate_public_manifest_invalid_version_tag(self):
        self.manifest["latest"]["resources"] = 123141241234  # type: ignore
        manifest_str = dumps(self.manifest)
        self.assertIsNone(deserialize_public_manifest(manifest_str))

    def test_validate_public_manifest_additional_entry(self):
        self.manifest["latest"]["extra"] = "v47826247-f7abf93e"  # type: ignore
        manifest_str = dumps(self.manifest)
        self.assertIsNone(deserialize_public_manifest(manifest_str))

    def test_validate_public_manifest_missing_entry(self) -> None:
        self.manifest["latest"].pop("resources")
        manifest_str = dumps(self.manifest)
        self.assertIsNone(deserialize_public_manifest(manifest_str))

    def test_validate_public_manifest_missing_version_history(self) -> None:
        del self.manifest["version_history"]  # type: ignore
        manifest_str = dumps(self.manifest)
        self.assertIsNone(deserialize_public_manifest(manifest_str))

    def test_validate_public_manifest_invalid_version_history(self) -> None:
        self.manifest["version_history"]["v47589963-6b32e37e"]["resources"] = (
            "v47589963-6b32e37e"  # version tag instead of hash
        )
        manifest_str = dumps(self.manifest)
        self.assertIsNone(deserialize_public_manifest(manifest_str))

    def test_validate_public_manifest_invalid_version_history_hash_length(self) -> None:
        self.manifest["version_history"]["v47589963-6b32e37e"]["resources"] = (
            "1238129374183"  # not a full 64 char hash (sha256)
        )
        manifest_str = dumps(self.manifest)
        self.assertIsNone(deserialize_public_manifest(manifest_str))
