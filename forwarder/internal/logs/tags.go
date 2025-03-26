package logs

import (
	// stdlib
	"strings"
	"sync"

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
	sourceTagMap   map[string]string
	sourceTagMu    sync.Mutex
	resourceTagMap map[string][]string
	resourceTagMu  sync.Mutex
)

func sourceTag(resourceType string) string {
	sourceTagMu.Lock()
	defer func() {
		sourceTagMu.Unlock()
	}()
	val, ok := sourceTagMap[resourceType]
	if ok {
		return val
	}
	parts := strings.Split(strings.ToLower(resourceType), "/")
	tag := strings.Replace(parts[0], "microsoft.", "azure.", -1)
	if sourceTagMap == nil {
		sourceTagMap = make(map[string]string)
	}
	sourceTagMap[resourceType] = tag
	return tag
}

func tagsFromResourceId(resourceId *arm.ResourceID) []string {
	if resourceId == nil {
		return nil
	}
	resourceTagMu.Lock()
	defer func() {
		resourceTagMu.Unlock()
	}()
	val, ok := resourceTagMap[resourceId.String()]
	if ok {
		return val
	}

	tags := []string{
		"subscription_id:" + resourceId.SubscriptionID,
		"source:" + sourceTag(resourceId.ResourceType.String()),
		"resource_group:" + resourceId.ResourceGroupName,
		"resource_type:" + resourceId.ResourceType.String(),
	}
	if resourceTagMap == nil {
		resourceTagMap = make(map[string][]string)
	}
	resourceTagMap[resourceId.String()] = tags
	return tags
}
