# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
import json
import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch as mock_patch

sys.path.insert(0, str(Path(__file__).parent.parent))

# project
import uninstall

SUB_ID_1 = "sub-1"
SUB_ID_2 = "sub-2"
SUB_ID_3 = "sub-3"

MOCK_SUBSCRIPTIONS = [
    {"id": SUB_ID_1, "name": "Development Subscription"},
    {"id": SUB_ID_2, "name": "Production Subscription"},
    {"id": SUB_ID_3, "name": "Test Subscription"},
]

MOCK_CONTROL_PLANE_STORAGE_ACCOUNTS = [
    {"resourceGroup": "dd-lfo-control-plane-rg", "name": "lfostoragef444ca0ac478"},
    {"resourceGroup": "dd-lfo-control-plane-rg-2", "name": "lfostoraged361rf3bew23"},
]

MOCK_FORWARDER_ENVIRONMENTS = [
    {"name": "dd-log-forwarder-env-f444ca0ac478-eastus", "resourceGroup": "dd-lfo-forwarder-rg"},
    {"name": "dd-log-forwarder-env-d361rf3bew23-westus", "resourceGroup": "dd-lfo-forwarder-rg-2"},
]

MOCK_ROLE_ASSIGNMENTS = [
    {
        "id": "/subscriptions/sub-1/providers/Microsoft.Authorization/roleAssignments/role-1",
        "roleDefinitionName": "Monitoring Contributor",
        "principalId": "principal-1",
        "principalName": "service-principal-1",
    },
    {
        "id": "/subscriptions/sub-1/providers/Microsoft.Authorization/roleAssignments/role-2",
        "roleDefinitionName": "Monitoring Reader",
        "principalId": "principal-2",
        "principalName": "service-principal-2",
    },
]

MOCK_RESOURCE_IDS = [
    "/subscriptions/sub-1/resourceGroups/rg-1/providers/Microsoft.Storage/storageAccounts/storage1",
    "/subscriptions/sub-1/resourceGroups/rg-2/providers/Microsoft.Compute/virtualMachines/vm1",
]

MOCK_DIAGNOSTIC_SETTINGS = {"datadog_log_forwarding_f444ca0ac478", "datadog_log_forwarding_d361rf3bew23"}


class TestUninstallScript(TestCase):
    def setUp(self) -> None:
        """Set up test fixtures and reset global settings"""
        # Reset global settings
        uninstall.DRY_RUN_SETTING = False
        uninstall.SKIP_PROMPTS_SETTING = False
        uninstall.CONTROL_PLANE_ID_SETTING = None
        uninstall.SUBSCRIPTION_ID_SETTING = None

        # Set up mocks
        self.az_mock = self.patch("uninstall.az")
        self.input_mock = self.patch("uninstall.input")
        self.log_mock = self.patch("uninstall.log")

    def patch(self, path: str, **kwargs):
        """Helper method to patch and auto-cleanup"""
        patcher = mock_patch(path, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def patch_path(self, path: str, **kwargs):
        """Helper method for full path patching"""
        patcher = mock_patch(path, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    # ===== Subscription Tests ===== #

    def test_list_users_subscriptions_success(self):
        """Test successful fetching of user subscriptions"""
        self.az_mock.return_value = json.dumps(MOCK_SUBSCRIPTIONS)

        result = uninstall.list_users_subscriptions()

        expected = {
            SUB_ID_1: "Development Subscription",
            SUB_ID_2: "Production Subscription",
            SUB_ID_3: "Test Subscription",
        }
        self.assertEqual(result, expected)
        self.az_mock.assert_called_once_with("account list --output json")

    # ===== Control Plane Discovery Tests ===== #

    def test_find_sub_control_planes_success(self):
        """Test finding control planes in a subscription"""
        self.az_mock.return_value = json.dumps(MOCK_CONTROL_PLANE_STORAGE_ACCOUNTS)

        result = uninstall.find_sub_control_planes(SUB_ID_1, "Test Subscription")

        expected = {
            "dd-lfo-control-plane-rg": "lfostoragef444ca0ac478",
            "dd-lfo-control-plane-rg-2": "lfostoraged361rf3bew23",
        }
        self.assertEqual(result, expected)

    def test_find_sub_control_planes_with_specific_id(self):
        """Test finding specific control plane by ID"""
        uninstall.CONTROL_PLANE_ID_SETTING = "f444ca0ac478"
        self.az_mock.return_value = json.dumps([MOCK_CONTROL_PLANE_STORAGE_ACCOUNTS[0]])

        result = uninstall.find_sub_control_planes(SUB_ID_1, "Test Subscription")

        expected = {"dd-lfo-control-plane-rg": "lfostoragef444ca0ac478"}
        self.assertEqual(result, expected)

        # Verify the command includes the specific control plane ID
        call_args = self.az_mock.call_args[0][0]
        self.assertIn("lfostoragef444ca0ac478", call_args)

    def test_find_sub_control_planes_auth_error(self):
        """Test handling authentication errors gracefully"""
        self.az_mock.side_effect = uninstall.AuthError("Authorization failed")

        result = uninstall.find_sub_control_planes(SUB_ID_1, "Test Subscription")

        self.assertEqual(result, {})

    def test_find_sub_control_planes_refresh_token_error(self):
        """Test handling token refresh errors gracefully"""
        self.az_mock.side_effect = uninstall.RefreshTokenError("Token expired")

        result = uninstall.find_sub_control_planes(SUB_ID_1, "Test Subscription")

        self.assertEqual(result, {})

    @mock_patch("uninstall.find_sub_control_planes")
    def test_find_all_control_planes_multiple_subscriptions(self, mock_find_sub):
        """Test finding control planes across multiple subscriptions"""
        # Mock the individual subscription results
        mock_find_sub.side_effect = [
            {"dd-lfo-control-plane-rg": "lfostoragef444ca0ac478"},  # sub-1
            {},  # sub-2 (no control planes)
            {"dd-lfo-control-plane-rg-2": "lfostoraged361rf3bew23"},  # sub-3
        ]

        sub_id_to_name = {
            SUB_ID_1: "Dev Subscription",
            SUB_ID_2: "Prod Subscription",
            SUB_ID_3: "Test Subscription",
        }

        sub_to_rg, rg_to_lfo_id = uninstall.find_all_control_planes(sub_id_to_name)

        expected_sub_to_rg = {
            SUB_ID_1: {"dd-lfo-control-plane-rg"},
            SUB_ID_3: {"dd-lfo-control-plane-rg-2"},
        }
        expected_rg_to_lfo_id = {
            "dd-lfo-control-plane-rg": {"f444ca0ac478"},
            "dd-lfo-control-plane-rg-2": {"d361rf3bew23"},
        }

        self.assertEqual(dict(sub_to_rg), expected_sub_to_rg)
        self.assertEqual(dict(rg_to_lfo_id), expected_rg_to_lfo_id)

    # ===== Subscription Filtering Tests ===== #

    def test_filter_subs_to_search_all_subscriptions(self):
        """Test filtering subscriptions when no specific subscription is set"""
        sub_id_to_name = {SUB_ID_1: "Sub 1", SUB_ID_2: "Sub 2"}

        result = uninstall.filter_subs_to_search(sub_id_to_name, "control planes")

        self.assertEqual(result, sub_id_to_name)

    def test_filter_subs_to_search_specific_subscription(self):
        """Test filtering to specific subscription"""
        uninstall.SUBSCRIPTION_ID_SETTING = SUB_ID_1
        sub_id_to_name = {SUB_ID_1: "Sub 1", SUB_ID_2: "Sub 2"}

        result = uninstall.filter_subs_to_search(sub_id_to_name, "control planes")

        expected = {SUB_ID_1: "Sub 1"}
        self.assertEqual(result, expected)

    def test_filter_subs_to_search_invalid_subscription(self):
        """Test error when specified subscription doesn't exist"""
        uninstall.SUBSCRIPTION_ID_SETTING = "invalid-sub"
        sub_id_to_name = {SUB_ID_1: "Sub 1", SUB_ID_2: "Sub 2"}

        with self.assertRaises(SystemExit):
            uninstall.filter_subs_to_search(sub_id_to_name, "control planes")

    # ===== Forwarder Environment Tests ===== #

    def test_find_all_forwarder_envs_success(self):
        """Test finding forwarder environments"""
        self.az_mock.return_value = json.dumps(MOCK_FORWARDER_ENVIRONMENTS)

        sub_id_to_name = {SUB_ID_1: "Test Subscription"}

        sub_to_rg, rg_to_lfo_id = uninstall.find_all_forwarder_envs(sub_id_to_name)

        expected_sub_to_rg = {SUB_ID_1: {"dd-lfo-forwarder-rg", "dd-lfo-forwarder-rg-2"}}
        expected_rg_to_lfo_id = {
            "dd-lfo-forwarder-rg": {"f444ca0ac478"},
            "dd-lfo-forwarder-rg-2": {"d361rf3bew23"},
        }

        self.assertEqual(dict(sub_to_rg), expected_sub_to_rg)
        self.assertEqual(dict(rg_to_lfo_id), expected_rg_to_lfo_id)

    # ===== Role Assignment Tests ===== #

    def test_find_role_assignments_success(self):
        """Test finding role assignments with control plane IDs"""
        self.az_mock.return_value = json.dumps(MOCK_ROLE_ASSIGNMENTS)

        sub_id_to_name = {SUB_ID_1: "Test Sub"}
        control_plane_ids = {"f444ca0ac478", "d361rf3bew23"}
        result = uninstall.find_role_assignments(sub_id_to_name, control_plane_ids)

        expected = {SUB_ID_1: MOCK_ROLE_ASSIGNMENTS}
        self.assertEqual(result, expected)

        # Verify the query includes the control plane IDs
        call_args = self.az_mock.call_args[0][0]
        self.assertIn("ddlfof444ca0ac478", call_args)
        self.assertIn("ddlfod361rf3bew23", call_args)

    @mock_patch("uninstall.find_role_assignments")
    @mock_patch("uninstall.find_unknown_role_assignments")
    def test_mark_role_assignment_deletions(self, mock_find_unknown, mock_find_role):
        """Test marking role assignments for deletion"""
        mock_find_role.return_value = {SUB_ID_1: MOCK_ROLE_ASSIGNMENTS, SUB_ID_2: MOCK_ROLE_ASSIGNMENTS}
        mock_find_unknown.return_value = {}  # No unknown role assignments

        sub_id_to_name = {SUB_ID_1: "Test Sub", SUB_ID_2: "Another Sub"}
        lfo_id_deletions = {"f444ca0ac478"}

        result = uninstall.mark_role_assignment_deletions(sub_id_to_name, lfo_id_deletions)

        expected = {SUB_ID_1: MOCK_ROLE_ASSIGNMENTS, SUB_ID_2: MOCK_ROLE_ASSIGNMENTS}
        self.assertEqual(dict(result), expected)

    # ===== Diagnostic Settings Tests ===== #

    @mock_patch("uninstall.list_resources")
    def test_find_diagnostic_settings_success(self, mock_list_resources):
        """Test finding diagnostic settings for resources"""
        mock_list_resources.return_value = set(MOCK_RESOURCE_IDS)
        self.az_mock.return_value = json.dumps(list(MOCK_DIAGNOSTIC_SETTINGS))

        control_plane_ids = {"f444ca0ac478", "d361rf3bew23"}
        result = uninstall.find_diagnostic_settings(SUB_ID_1, "Test Sub", control_plane_ids)

        expected = {
            MOCK_RESOURCE_IDS[0]: MOCK_DIAGNOSTIC_SETTINGS,
            MOCK_RESOURCE_IDS[1]: MOCK_DIAGNOSTIC_SETTINGS,
        }
        self.assertEqual(result, expected)

    @mock_patch("uninstall.find_diagnostic_settings")
    def test_mark_diagnostic_setting_deletions(self, mock_find_diag):
        """Test marking diagnostic settings for deletion"""
        mock_diagnostic_map = {"/resource/1": {"diag-1", "diag-2"}, "/resource/2": {"diag-3"}}
        mock_find_diag.return_value = mock_diagnostic_map

        sub_id_to_name = {SUB_ID_1: "Test Sub"}
        lfo_id_deletions = {"f444ca0ac478"}

        result = uninstall.mark_diagnostic_setting_deletions(sub_id_to_name, lfo_id_deletions)

        expected = {SUB_ID_1: mock_diagnostic_map}
        self.assertEqual(dict(result), expected)

    # ===== LFO ID Deletion Tests ===== #

    def test_mark_lfo_id_deletions_with_control_plane_setting(self):
        """Test marking LFO IDs for deletion when control plane ID is set"""
        uninstall.CONTROL_PLANE_ID_SETTING = "f444ca0ac478"

        sub_to_rg_deletions = {SUB_ID_1: {"rg-1"}}
        rg_to_lfo_ids = {"rg-1": {"f444ca0ac478", "d361rf3bew23"}}

        result = uninstall.mark_lfo_id_deletions(sub_to_rg_deletions, rg_to_lfo_ids)

        self.assertEqual(result, {"f444ca0ac478"})

    def test_mark_lfo_id_deletions_from_resource_groups(self):
        """Test marking LFO IDs for deletion based on resource groups"""
        uninstall.CONTROL_PLANE_ID_SETTING = None

        sub_to_rg_deletions = {SUB_ID_1: {"rg-1", "rg-2"}, SUB_ID_2: {"rg-3"}}
        rg_to_lfo_ids = {"rg-1": {"f444ca0ac478"}, "rg-2": {"d361rf3bew23"}, "rg-3": {"cp789"}}

        result = uninstall.mark_lfo_id_deletions(sub_to_rg_deletions, rg_to_lfo_ids)

        expected = {"f444ca0ac478", "d361rf3bew23", "cp789"}
        self.assertEqual(result, expected)

    # ===== Resource Group Tests ===== #

    def test_sub_has_rg_exists(self):
        """Test checking if subscription has resource group - exists"""
        self.az_mock.return_value = "true"

        result = uninstall.sub_has_rg(SUB_ID_1, "test-rg")

        self.assertTrue(result)
        self.az_mock.assert_called_once_with("group exists --name test-rg --subscription sub-1")

    def test_sub_has_rg_does_not_exist(self):
        """Test checking if subscription has resource group - doesn't exist"""
        self.az_mock.return_value = "false"

        result = uninstall.sub_has_rg(SUB_ID_1, "test-rg")

        self.assertFalse(result)

    def test_num_resources_in_group_success(self):
        """Test counting resources in a resource group"""
        self.az_mock.return_value = "5"

        result = uninstall.num_resources_in_group(SUB_ID_1, "test-rg")

        self.assertEqual(result, 5)

    def test_list_resources_success(self):
        """Test listing resources in a subscription"""
        self.az_mock.return_value = json.dumps(MOCK_RESOURCE_IDS)

        result = uninstall.list_resources(SUB_ID_1, "Test Sub")

        self.assertEqual(result, set(MOCK_RESOURCE_IDS))

    # ===== Deletion Tests ===== #

    def test_delete_resource_group_dry_run(self):
        """Test resource group deletion in dry run mode"""
        uninstall.DRY_RUN_SETTING = True

        uninstall.delete_resource_group(SUB_ID_1, "test-rg")

        self.az_mock.assert_not_called()

    def test_delete_resource_group_actual(self):
        """Test actual resource group deletion"""
        uninstall.DRY_RUN_SETTING = False

        uninstall.delete_resource_group(SUB_ID_1, "test-rg")

        expected_cmd = "group delete --subscription sub-1 --name test-rg --yes --no-wait"
        self.az_mock.assert_called_once_with(expected_cmd)

    def test_delete_role_assignments_dry_run(self):
        """Test role assignment deletion in dry run mode"""
        uninstall.DRY_RUN_SETTING = True

        uninstall.delete_role_assignments(SUB_ID_1, MOCK_ROLE_ASSIGNMENTS)

        self.az_mock.assert_not_called()

    def test_delete_role_assignments_actual(self):
        """Test actual role assignment deletion"""
        uninstall.DRY_RUN_SETTING = False

        uninstall.delete_role_assignments(SUB_ID_1, MOCK_ROLE_ASSIGNMENTS)

        self.az_mock.assert_called_once()

        actual_cmd = self.az_mock.call_args[0][0]

        # Verify it contains the expected components (order of IDs may vary due to set usage)
        self.assertIn("role assignment delete --ids", actual_cmd)
        self.assertIn("/subscriptions/sub-1/providers/Microsoft.Authorization/roleAssignments/role-1", actual_cmd)
        self.assertIn("/subscriptions/sub-1/providers/Microsoft.Authorization/roleAssignments/role-2", actual_cmd)
        self.assertIn("--subscription sub-1 --include-inherited --yes", actual_cmd)

    def test_delete_diagnostic_setting_dry_run(self):
        """Test diagnostic setting deletion in dry run mode"""
        uninstall.DRY_RUN_SETTING = True

        uninstall.delete_diagnostic_setting(SUB_ID_1, "/resource/1", "diag-setting-1")

        self.az_mock.assert_not_called()

    def test_delete_diagnostic_setting_actual(self):
        """Test actual diagnostic setting deletion"""
        uninstall.DRY_RUN_SETTING = False

        uninstall.delete_diagnostic_setting(SUB_ID_1, "/resource/1", "diag-setting-1")

        expected_cmd = (
            "monitor diagnostic-settings delete --name diag-setting-1 --resource /resource/1 --subscription sub-1"
        )
        self.az_mock.assert_called_once_with(expected_cmd)

    # ===== User Interaction Tests ===== #

    def test_choose_rgs_to_delete_all(self):
        """Test choosing all resource groups for deletion"""
        self.input_mock.return_value = "*"

        resource_groups = {"rg-1", "rg-2", "rg-3"}
        result = uninstall.choose_rgs_to_delete(resource_groups)

        self.assertEqual(result, resource_groups)

    def test_choose_rgs_to_delete_none(self):
        """Test choosing no resource groups for deletion"""
        self.input_mock.return_value = "-"

        resource_groups = {"rg-1", "rg-2", "rg-3"}
        result = uninstall.choose_rgs_to_delete(resource_groups)

        self.assertEqual(result, set())

    def test_choose_rgs_to_delete_specific(self):
        """Test choosing specific resource group for deletion"""
        self.input_mock.return_value = "rg-2"

        resource_groups = {"rg-1", "rg-2", "rg-3"}
        result = uninstall.choose_rgs_to_delete(resource_groups)

        self.assertEqual(result, {"rg-2"})

    def test_choose_rgs_to_delete_skip_prompts(self):
        """Test choosing resource groups when prompts are skipped"""
        uninstall.SKIP_PROMPTS_SETTING = True

        resource_groups = {"rg-1", "rg-2", "rg-3"}
        result = uninstall.choose_rgs_to_delete(resource_groups)

        self.assertEqual(result, resource_groups)

    # ===== Utility Function Tests ===== #

    def test_utility_functions(self):
        """Test various utility functions"""
        # Test space_separated
        self.assertEqual(uninstall.space_separated(["a", "b", "c"]), "a b c")

        # Test formatted_number
        self.assertEqual(uninstall.formatted_number(1000), "1,000")
        self.assertEqual(uninstall.formatted_number(1234567), "1,234,567")

        # Test dry_run_of
        self.assertEqual(uninstall.dry_run_of("Deleting resource"), "DRY RUN | Would be deleting resource")

        # Test first_key_of
        self.assertEqual(uninstall.first_key_of({"a": 1, "b": 2}), "a")

        with self.assertRaises(ValueError):
            uninstall.first_key_of({})


if __name__ == "__main__":
    import unittest

    unittest.main()
