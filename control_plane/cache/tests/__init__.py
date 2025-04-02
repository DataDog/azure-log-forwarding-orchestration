# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from os import environ

TEST_CONNECTION_STRING = "TEST_CONNECTION_STRING"
TEST_EVENT_HUB_NAME = "TEST_EVENT_HUB"
TEST_EVENT_HUB_NAMESPACE = "TEST_EVENT_HUB_NAMESPACE"

environ.setdefault("AzureWebJobsStorage", TEST_CONNECTION_STRING)
environ.setdefault("EVENT_HUB_NAME", TEST_EVENT_HUB_NAME)
environ.setdefault("EVENT_HUB_NAMESPACE", TEST_EVENT_HUB_NAMESPACE)

sub_id1 = "f7a0a345-103e-4b6e-93b7-13d0a56fd363"
sub_id2 = "422f4b27-5493-4450-b308-0118e123ca89"
