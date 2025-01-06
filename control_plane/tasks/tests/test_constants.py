# stdlib
from unittest import TestCase

# project
from tasks.constants import ALLOWED_CONTAINER_APP_REGIONS, ALLOWED_STORAGE_ACCOUNT_REGIONS


class TestConstants(TestCase):
    def test_all_allowed_container_app_regions_are_allowed_storage_regions(self):
        # no regions should be allowed for container apps that aren't allowed for storage accounts
        self.assertEqual(ALLOWED_CONTAINER_APP_REGIONS - ALLOWED_STORAGE_ACCOUNT_REGIONS, frozenset())
