package logs

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
)

type Log struct {
	ByteSize   int             `json:"-"`
	Json       json.RawMessage `json:"-"`
	ResourceId string          `json:"resourceId"`
	Category   string          `json:"category"`
	Tags       []string        `json:"tags"`
}

func trimQuotes(s string) string {
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
	}
	delete(tempJson, "resourceId")

	logJson, err := json.Marshal(tempJson)
	if err != nil {
		return nil, err
	}
	return &Log{
		ByteSize:   len(azureLog),
		Category:   category,
		ResourceId: resourceId,
		Json:       logJson,
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
