package logs

import (
	// stdlib
	"bytes"
	"encoding/json"
	"strings"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
)

type Log struct {
	ByteSize   int
	Content    string
	ResourceId string
	Category   string
	Tags       []string
}

func trimQuotes(s string) string {
	// TODO AZINTS-2751 replace with json5 parsing
	if len(s) < 2 {
		return s
	}
	if s[0] == '"' {
		s = s[1:]
	}
	if i := len(s) - 1; s[i] == '"' {
		s = s[:i]
	}
	return s
}

func unmarshall(azureLog []byte) (*Log, error) {
	tempJson := make(map[string]json.RawMessage)
	if err := json.Unmarshal(azureLog, &tempJson); err != nil {
		return nil, err
	}

	var category string
	categoryBytes := tempJson["category"]
	if categoryBytes != nil {
		category = trimQuotes(string(categoryBytes))
	}
	delete(tempJson, "category")

	var resourceId string
	resourceIdBytes := tempJson["resourceId"]
	if resourceIdBytes != nil {
		resourceId = trimQuotes(string(resourceIdBytes))
		delete(tempJson, "resourceId")
	}
	if resourceId == "" {
		resourceIdBytes = tempJson["ResourceId"]
		if resourceIdBytes != nil {
			resourceId = trimQuotes(string(resourceIdBytes))
			delete(tempJson, "ResourceId")
		}
	}
	if resourceId == "" {
		resourceIdBytes = tempJson["resourceID"]
		if resourceIdBytes != nil {
			resourceId = trimQuotes(string(resourceIdBytes))
			delete(tempJson, "resourceID")
		}

	}

	logJson, err := json.Marshal(tempJson)
	if err != nil {
		return nil, err
	}
	return &Log{
		ByteSize:   len(azureLog),
		Category:   category,
		ResourceId: resourceId,
		Content:    string(logJson),
	}, err
}

func getResourceIdTags(id *arm.ResourceID) []string {
	var tags []string

	tags = append(tags, "subscription_id:"+id.SubscriptionID)
	tags = append(tags, "resource_group:"+id.ResourceGroupName)
	tags = append(tags, "source:"+strings.Replace(id.ResourceType.String(), "/", ".", -1))

	return tags
}

func getForwarderTags() []string {
	return []string{"forwarder:lfo"}
}

// NewLog creates a new Log from the given log bytes
func NewLog(logBytes []byte) (*Log, error) {
	logBytes = bytes.ReplaceAll(logBytes, []byte("'"), []byte("\""))
	log, err := unmarshall(logBytes)
	if err != nil {
		return nil, err
	}
	parsedId, err := arm.ParseResourceID(log.ResourceId)
	if err != nil {
		return nil, err
	}

	log.Tags = getResourceIdTags(parsedId)
	log.Tags = append(log.Tags, getForwarderTags()...)

	return log, nil
}
