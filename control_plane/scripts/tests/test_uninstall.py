# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
import concurrent.futures
import json
import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch as mock_patch

# Needed to import the uninstall script
sys.path.insert(0, str(Path(__file__).parent.parent))

# project
import uninstall

SUB_ID_1 = "sub-1"
SUB_ID_2 = "sub-2"
SUB_ID_3 = "sub-3"
SUB_NAME_1 = "Development Subscription"
SUB_NAME_2 = "Production Subscription"
SUB_NAME_3 = "Test Subscription"
CONTROL_PLANE_ID_1 = "f444ca0ac478"
CONTROL_PLANE_ID_2 = "d361rf3bew23"
CONTROL_PLANE_ID_3 = "4d6h2vyu5p78"
CONTROL_PLANE_RESOURCE_GROUP_1 = "dd-lfo-control-plane-rg"
CONTROL_PLANE_RESOURCE_GROUP_2 = "dd-lfo-control-plane-rg-2"
CONTROL_PLANE_RESOURCE_GROUP_3 = "dd-lfo-control-plane-rg-3"
FORWARDER_NAME_1 = f"dd-log-forwarder-env-{CONTROL_PLANE_ID_1}-eastus"
FORWARDER_NAME_2 = f"dd-log-forwarder-env-{CONTROL_PLANE_ID_2}-westus"
FORWARDER_RESOURCE_GROUP_1 = "forwarder-rg"
FORWARDER_RESOURCE_GROUP_2 = "forwarder-rg-2"

MOCK_SUBSCRIPTIONS = [
    {"id": SUB_ID_1, "name": SUB_NAME_1},
    {"id": SUB_ID_2, "name": SUB_NAME_2},
    {"id": SUB_ID_3, "name": SUB_NAME_3},
]

MOCK_CONTROL_PLANE_STORAGE_ACCOUNTS = [
    {"resourceGroup": CONTROL_PLANE_RESOURCE_GROUP_1, "name": f"lfostorage{CONTROL_PLANE_ID_1}"},
    {"resourceGroup": CONTROL_PLANE_RESOURCE_GROUP_2, "name": f"lfostorage{CONTROL_PLANE_ID_2}"},
]

MOCK_FORWARDER_ENVIRONMENTS = [
    {"name": FORWARDER_NAME_1, "resourceGroup": FORWARDER_RESOURCE_GROUP_1},
    {"name": FORWARDER_NAME_2, "resourceGroup": FORWARDER_RESOURCE_GROUP_2},
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

MOCK_UNKNOWN_ROLE_ASSIGNMENT = {
    "id": "/subscriptions/sub-1/providers/Microsoft.Authorization/roleAssignments/unknown-role-1",
    "roleDefinitionName": "Monitoring Contributor",
    "principalId": "unknown-principal-1",
    "principalName": "",  # Empty principalName indicates "Unknown" role assignbment
}

MOCK_RESOURCE_IDS = [
    "/subscriptions/sub-1/resourceGroups/test-rg-1/providers/Microsoft.Storage/storageAccounts/storage1",
    "/subscriptions/sub-1/resourceGroups/test-rg-2/providers/Microsoft.Network/loadBalancers/lb1",
]

MOCK_DIAGNOSTIC_SETTINGS = {
    f"datadog_log_forwarding_{CONTROL_PLANE_ID_1}",
    f"datadog_log_forwarding_{CONTROL_PLANE_ID_2}",
}


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
        self.az_mock.return_value = json.dumps(MOCK_SUBSCRIPTIONS)

        result = uninstall.list_users_subscriptions()

        expected = {
            SUB_ID_1: SUB_NAME_1,
            SUB_ID_2: SUB_NAME_2,
            SUB_ID_3: SUB_NAME_3,
        }
        self.assertEqual(result, expected)
        self.az_mock.assert_called_once_with("account list --output json")

    # ===== Control Plane Discovery Tests ===== #

    def test_find_sub_control_planes_success(self):
        """Test finding control planes in a subscription"""
        self.az_mock.return_value = json.dumps(MOCK_CONTROL_PLANE_STORAGE_ACCOUNTS)

        result = uninstall.find_sub_control_planes(SUB_ID_1, SUB_NAME_1)

        expected = {
            CONTROL_PLANE_RESOURCE_GROUP_1: f"lfostorage{CONTROL_PLANE_ID_1}",
            CONTROL_PLANE_RESOURCE_GROUP_2: f"lfostorage{CONTROL_PLANE_ID_2}",
        }
        self.assertEqual(result, expected)

    def test_find_sub_control_planes_with_specific_id(self):
        """Test finding specific control plane by ID"""
        uninstall.CONTROL_PLANE_ID_SETTING = CONTROL_PLANE_ID_1
        self.az_mock.return_value = json.dumps([MOCK_CONTROL_PLANE_STORAGE_ACCOUNTS[0]])

        result = uninstall.find_sub_control_planes(SUB_ID_1, SUB_NAME_1)

        expected = {CONTROL_PLANE_RESOURCE_GROUP_1: f"lfostorage{CONTROL_PLANE_ID_1}"}
        self.assertEqual(result, expected)

        # Verify the command includes the specific control plane ID
        call_args = self.az_mock.call_args[0][0]
        self.assertIn(f"lfostorage{CONTROL_PLANE_ID_1}", call_args)

    def test_find_sub_control_planes_auth_error(self):
        """Test handling authentication errors gracefully"""
        self.az_mock.side_effect = uninstall.AuthError("Authorization failed")

        result = uninstall.find_sub_control_planes(SUB_ID_1, SUB_NAME_1)

        self.assertEqual(result, {})

    def test_find_sub_control_planes_refresh_token_error(self):
        """Test handling token refresh errors gracefully"""
        self.az_mock.side_effect = uninstall.RefreshTokenError("Token expired")

        result = uninstall.find_sub_control_planes(SUB_ID_1, SUB_NAME_1)

        self.assertEqual(result, {})

    @mock_patch("uninstall.find_sub_control_planes")
    def test_find_all_control_planes_multiple_subscriptions(self, mock_find_sub):
        """Test finding control planes across multiple subscriptions"""
        # Mock the individual subscription results
        mock_find_sub.side_effect = [
            {CONTROL_PLANE_RESOURCE_GROUP_1: f"lfostorage{CONTROL_PLANE_ID_1}"},
            {},  # no control planes
            {CONTROL_PLANE_RESOURCE_GROUP_2: f"lfostorage{CONTROL_PLANE_ID_2}"},
        ]

        sub_id_to_name = {
            SUB_ID_1: SUB_NAME_1,
            SUB_ID_2: SUB_NAME_2,
            SUB_ID_3: SUB_NAME_3,
        }

        sub_to_rg, rg_to_lfo_id = uninstall.find_all_control_planes(sub_id_to_name)

        expected_sub_to_rg = {
            SUB_ID_1: {CONTROL_PLANE_RESOURCE_GROUP_1},
            SUB_ID_3: {CONTROL_PLANE_RESOURCE_GROUP_2},
        }
        expected_rg_to_lfo_id = {
            CONTROL_PLANE_RESOURCE_GROUP_1: {CONTROL_PLANE_ID_1},
            CONTROL_PLANE_RESOURCE_GROUP_2: {CONTROL_PLANE_ID_2},
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

        sub_id_to_name = {SUB_ID_1: SUB_NAME_1}

        sub_to_rg, rg_to_lfo_id = uninstall.find_all_forwarder_envs(sub_id_to_name)

        expected_sub_to_rg = {SUB_ID_1: {FORWARDER_RESOURCE_GROUP_1, FORWARDER_RESOURCE_GROUP_2}}
        expected_rg_to_lfo_id = {
            FORWARDER_RESOURCE_GROUP_1: {CONTROL_PLANE_ID_1},
            FORWARDER_RESOURCE_GROUP_2: {CONTROL_PLANE_ID_2},
        }

        self.assertEqual(dict(sub_to_rg), expected_sub_to_rg)
        self.assertEqual(dict(rg_to_lfo_id), expected_rg_to_lfo_id)

    # ===== Role Assignment Tests ===== #

    def test_find_role_assignments_success(self):
        """Test finding role assignments with control plane IDs"""
        self.az_mock.return_value = json.dumps(MOCK_ROLE_ASSIGNMENTS)

        sub_id_to_name = {SUB_ID_1: SUB_NAME_1}
        control_plane_ids = {CONTROL_PLANE_ID_1, CONTROL_PLANE_ID_2}
        result = uninstall.find_role_assignments(sub_id_to_name, control_plane_ids)

        expected = {SUB_ID_1: MOCK_ROLE_ASSIGNMENTS}
        self.assertEqual(result, expected)

        # Verify the query includes the control plane IDs
        call_args = self.az_mock.call_args[0][0]
        self.assertIn(f"ddlfo{CONTROL_PLANE_ID_1}", call_args)
        self.assertIn(f"ddlfo{CONTROL_PLANE_ID_2}", call_args)

    @mock_patch("uninstall.find_role_assignments")
    @mock_patch("uninstall.find_unknown_role_assignments")
    def test_mark_role_assignment_deletions(self, mock_find_unknown, mock_find_role):
        """Test marking role assignments for deletion"""
        mock_find_role.return_value = {SUB_ID_1: MOCK_ROLE_ASSIGNMENTS.copy(), SUB_ID_2: MOCK_ROLE_ASSIGNMENTS.copy()}

        mock_find_unknown.return_value = {SUB_ID_1: [MOCK_UNKNOWN_ROLE_ASSIGNMENT]}

        sub_id_to_name = {sub["id"]: sub["name"] for sub in MOCK_SUBSCRIPTIONS}
        lfo_id_deletions = {CONTROL_PLANE_ID_1}

        result = uninstall.mark_role_assignment_deletions(sub_id_to_name, lfo_id_deletions)

        expected_sub1 = MOCK_ROLE_ASSIGNMENTS.copy()
        expected_sub1.extend([MOCK_UNKNOWN_ROLE_ASSIGNMENT])

        expected = {
            SUB_ID_1: expected_sub1,
            SUB_ID_2: MOCK_ROLE_ASSIGNMENTS,
        }
        self.assertEqual(dict(result), expected)

        mock_find_role.assert_called_once_with(sub_id_to_name, lfo_id_deletions)
        mock_find_unknown.assert_called_once_with(sub_id_to_name)

    # ===== Diagnostic Settings Tests ===== #

    @mock_patch("uninstall.list_resources")
    def test_find_diagnostic_settings_success(self, mock_list_resources):
        """Test finding diagnostic settings for resources"""
        mock_list_resources.return_value = set(MOCK_RESOURCE_IDS)
        self.az_mock.return_value = json.dumps(list(MOCK_DIAGNOSTIC_SETTINGS))

        control_plane_ids = {CONTROL_PLANE_ID_1, CONTROL_PLANE_ID_2}
        result = uninstall.find_diagnostic_settings(SUB_ID_1, SUB_NAME_1, control_plane_ids)

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

        sub_id_to_name = {SUB_ID_1: SUB_NAME_1}
        lfo_id_deletions = {CONTROL_PLANE_ID_1}

        result = uninstall.mark_diagnostic_setting_deletions(sub_id_to_name, lfo_id_deletions)

        expected = {SUB_ID_1: mock_diagnostic_map}
        self.assertEqual(dict(result), expected)

    # ===== LFO ID Deletion Tests ===== #

    def test_mark_lfo_id_deletions_with_control_plane_setting(self):
        """Test marking LFO IDs for deletion when control plane ID is set"""
        uninstall.CONTROL_PLANE_ID_SETTING = CONTROL_PLANE_ID_1

        sub_to_rg_deletions = {SUB_ID_1: {CONTROL_PLANE_RESOURCE_GROUP_1}}
        rg_to_lfo_ids = {CONTROL_PLANE_RESOURCE_GROUP_1: {CONTROL_PLANE_ID_1, CONTROL_PLANE_ID_2}}

        result = uninstall.mark_lfo_id_deletions(sub_to_rg_deletions, rg_to_lfo_ids)

        self.assertEqual(result, {CONTROL_PLANE_ID_1})

    def test_mark_lfo_id_deletions_from_resource_groups(self):
        """Test marking LFO IDs for deletion based on resource groups"""
        uninstall.CONTROL_PLANE_ID_SETTING = None

        sub_to_rg_deletions = {
            SUB_ID_1: {CONTROL_PLANE_RESOURCE_GROUP_1, CONTROL_PLANE_RESOURCE_GROUP_2},
            SUB_ID_2: {CONTROL_PLANE_RESOURCE_GROUP_3},
        }
        rg_to_lfo_ids = {
            CONTROL_PLANE_RESOURCE_GROUP_1: {CONTROL_PLANE_ID_1},
            CONTROL_PLANE_RESOURCE_GROUP_2: {CONTROL_PLANE_ID_2},
            CONTROL_PLANE_RESOURCE_GROUP_3: {CONTROL_PLANE_ID_3},
        }

        result = uninstall.mark_lfo_id_deletions(sub_to_rg_deletions, rg_to_lfo_ids)

        expected = {CONTROL_PLANE_ID_1, CONTROL_PLANE_ID_2, CONTROL_PLANE_ID_3}
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

        result = uninstall.list_resources(SUB_ID_1, SUB_NAME_1)

        self.assertEqual(result, set(MOCK_RESOURCE_IDS))

    # ===== Deletion Tests ===== #

    def test_delete_resource_group_dry_run(self):
        """Test resource group deletion in dry run mode"""
        uninstall.DRY_RUN_SETTING = True

        uninstall.delete_resource_group(SUB_ID_1, CONTROL_PLANE_RESOURCE_GROUP_1)

        self.az_mock.assert_not_called()

    def test_delete_resource_group_actual(self):
        """Test actual resource group deletion"""
        uninstall.DRY_RUN_SETTING = False

        uninstall.delete_resource_group(SUB_ID_1, CONTROL_PLANE_RESOURCE_GROUP_1)

        expected_cmd = f"group delete --subscription {SUB_ID_1} --name {CONTROL_PLANE_RESOURCE_GROUP_1} --yes --no-wait"
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
        self.assertIn(f"/subscriptions/{SUB_ID_1}/providers/Microsoft.Authorization/roleAssignments/role-1", actual_cmd)
        self.assertIn(f"/subscriptions/{SUB_ID_1}/providers/Microsoft.Authorization/roleAssignments/role-2", actual_cmd)
        self.assertIn(f"--subscription {SUB_ID_1} --include-inherited --yes", actual_cmd)

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
            f"monitor diagnostic-settings delete --name diag-setting-1 --resource /resource/1 --subscription {SUB_ID_1}"
        )
        self.az_mock.assert_called_once_with(expected_cmd)

    # ===== User Interaction Tests ===== #

    def test_choose_rgs_to_delete_all(self):
        """Test choosing all resource groups for deletion"""
        self.input_mock.return_value = "*"

        resource_groups = {
            CONTROL_PLANE_RESOURCE_GROUP_1,
            CONTROL_PLANE_RESOURCE_GROUP_2,
            CONTROL_PLANE_RESOURCE_GROUP_3,
        }
        result = uninstall.choose_rgs_to_delete(resource_groups)

        self.assertEqual(result, resource_groups)

    def test_choose_rgs_to_delete_none(self):
        """Test choosing no resource groups for deletion"""
        self.input_mock.return_value = "-"

        resource_groups = {
            CONTROL_PLANE_RESOURCE_GROUP_1,
            CONTROL_PLANE_RESOURCE_GROUP_2,
            CONTROL_PLANE_RESOURCE_GROUP_3,
        }
        result = uninstall.choose_rgs_to_delete(resource_groups)

        self.assertEqual(result, set())

    def test_choose_rgs_to_delete_specific(self):
        """Test choosing specific resource group for deletion"""
        self.input_mock.return_value = CONTROL_PLANE_RESOURCE_GROUP_2

        resource_groups = {
            CONTROL_PLANE_RESOURCE_GROUP_1,
            CONTROL_PLANE_RESOURCE_GROUP_2,
            CONTROL_PLANE_RESOURCE_GROUP_3,
        }
        result = uninstall.choose_rgs_to_delete(resource_groups)

        self.assertEqual(result, {CONTROL_PLANE_RESOURCE_GROUP_2})

    def test_choose_rgs_to_delete_skip_prompts(self):
        """Test choosing resource groups when prompts are skipped"""
        uninstall.SKIP_PROMPTS_SETTING = True

        resource_groups = {
            CONTROL_PLANE_RESOURCE_GROUP_1,
            CONTROL_PLANE_RESOURCE_GROUP_2,
            CONTROL_PLANE_RESOURCE_GROUP_3,
        }
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

    # ===== Timeout Tests ===== #

    @mock_patch("uninstall.ThreadPoolExecutor")
    def test_find_all_control_planes_timeout(self, mock_executor_class):
        """Test that find_all_control_planes handles timeout gracefully"""
        # Create a mock future that raises TimeoutError
        mock_future = MagicMock()
        mock_future.done.return_value = True
        mock_future.exception.return_value = None
        mock_future.result.side_effect = concurrent.futures.TimeoutError()

        mock_executor = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=False)
        mock_executor.submit.return_value = mock_future
        mock_executor_class.return_value = mock_executor

        sub_id_to_name = {SUB_ID_1: SUB_NAME_1}

        sub_to_rg, rg_to_lfo_id = uninstall.find_all_control_planes(sub_id_to_name)

        # Should return empty results due to timeout
        self.assertEqual(dict(sub_to_rg), {})
        self.assertEqual(dict(rg_to_lfo_id), {})
        self.log_mock.error.assert_called()
        # Verify the error message mentions timeout
        error_calls = [str(call) for call in self.log_mock.error.call_args_list]
        self.assertTrue(any("Timeout" in call for call in error_calls))

    @mock_patch("uninstall.ThreadPoolExecutor")
    def test_find_role_assignments_timeout(self, mock_executor_class):
        """Test that find_role_assignments handles timeout gracefully"""
        # Create a mock future that raises TimeoutError
        mock_future = MagicMock()
        mock_future.done.return_value = True
        mock_future.exception.return_value = None
        mock_future.result.side_effect = concurrent.futures.TimeoutError()

        mock_executor = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=False)
        mock_executor.submit.return_value = mock_future
        mock_executor_class.return_value = mock_executor

        sub_id_to_name = {SUB_ID_1: SUB_NAME_1}
        control_plane_ids = {CONTROL_PLANE_ID_1}

        result = uninstall.find_role_assignments(sub_id_to_name, control_plane_ids)

        # Should return empty results due to timeout
        self.assertEqual(result, {})
        self.log_mock.error.assert_called()
        # Verify the error message mentions timeout
        error_calls = [str(call) for call in self.log_mock.error.call_args_list]
        self.assertTrue(any("Timeout" in call for call in error_calls))

    @mock_patch("uninstall.ThreadPoolExecutor")
    def test_find_unknown_role_assignments_timeout(self, mock_executor_class):
        """Test that find_unknown_role_assignments handles timeout gracefully"""
        # Create a mock future that raises TimeoutError
        mock_future = MagicMock()
        mock_future.done.return_value = True
        mock_future.exception.return_value = None
        mock_future.result.side_effect = concurrent.futures.TimeoutError()

        mock_executor = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=False)
        mock_executor.submit.return_value = mock_future
        mock_executor_class.return_value = mock_executor

        sub_id_to_name = {SUB_ID_1: SUB_NAME_1}

        result = uninstall.find_unknown_role_assignments(sub_id_to_name)

        # Should return empty results due to timeout
        self.assertEqual(result, {})
        self.log_mock.error.assert_called()
        # Verify the error message mentions timeout
        error_calls = [str(call) for call in self.log_mock.error.call_args_list]
        self.assertTrue(any("Timeout" in call for call in error_calls))

    @mock_patch("uninstall.ThreadPoolExecutor")
    @mock_patch("uninstall.list_resources")
    def test_find_diagnostic_settings_timeout(self, mock_list_resources, mock_executor_class):
        """Test that find_diagnostic_settings handles timeout gracefully"""
        mock_list_resources.return_value = set(MOCK_RESOURCE_IDS)

        # Create a mock future that raises TimeoutError
        mock_future = MagicMock()
        mock_future.done.return_value = True
        mock_future.result.side_effect = concurrent.futures.TimeoutError()

        mock_executor = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=False)
        mock_executor.submit.return_value = mock_future
        mock_executor_class.return_value = mock_executor

        control_plane_ids = {CONTROL_PLANE_ID_1, CONTROL_PLANE_ID_2}

        result = uninstall.find_diagnostic_settings(SUB_ID_1, SUB_NAME_1, control_plane_ids)

        # Should return empty results due to timeout on all resources
        self.assertEqual(result, {})
        self.log_mock.error.assert_called()
        # Verify the error message mentions timeout
        error_calls = [str(call) for call in self.log_mock.error.call_args_list]
        self.assertTrue(any("Timeout" in call for call in error_calls))
