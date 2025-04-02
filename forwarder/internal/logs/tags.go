// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs

import (
	// stdlib
	"strings"
	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
)

var (
	// DefaultTags are the tags to include with every log.
	DefaultTags = []string{
		"forwarder:lfo",
		"control_plane_id:" + environment.Get(environment.ControlPlaneId),
		"config_id:" + environment.Get(environment.ConfigId),
	}
)

func sourceTag(resourceType string) string {
	parts := strings.Split(strings.ToLower(resourceType), "/")
	return strings.Replace(parts[0], "microsoft.", "azure.", -1)
}

func tagsFromResourceId(resourceId *arm.ResourceID) []string {
	if resourceId == nil {
		return nil
	}
	return []string{
		"subscription_id:" + resourceId.SubscriptionID,
		"source:" + sourceTag(resourceId.ResourceType.String()),
		"resource_group:" + resourceId.ResourceGroupName,
		"resource_type:" + resourceId.ResourceType.String(),
	}
}
