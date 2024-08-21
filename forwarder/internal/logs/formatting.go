package logs

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
)

type Log struct {
	ByteSize int `json:"-"`
	//DDRequire     DDLogs
	Json       json.RawMessage `json:"-"`
	ResourceId string          `json:"resourceId"`
	Category   string          `json:"category"`
	Tags       []string        `json:"tags"`
}

//type DDLogs struct {
//	ResourceId       string `json:"resourceId"` // important
//	Category         string `json:"category"`
//	DDSource         string `json:"ddsource"`
//	DDSourceCategory string `json:"ddsourcecategory"`
//	Service          string `json:"service"`
//	DDTags           string `json:"ddtags"` // string array of tags
//}

func TrimQuotes(s string) string {
	if s[0] == '"' {
		s = s[1:]
	}
	if i := len(s) - 1; s[i] == '"' {
		s = s[:i]
	}
	return s
}

func unmarshallToPartialStruct(azureLog []byte) (*Log, error) {
	var err error
	// partially unmarshall json to struct and keep remaining data as Raw json
	tempJson := make(map[string]json.RawMessage)
	if err = json.Unmarshal(azureLog, &tempJson); err != nil {
		return nil, err
	}

	var category string
	categoryBytes := tempJson["category"]
	if categoryBytes != nil {
		category = TrimQuotes(string(categoryBytes))
	}
	delete(tempJson, "category")

	var resourceId string
	resourceIdBytes := tempJson["resourceId"]
	if resourceIdBytes != nil {
		resourceId = TrimQuotes(string(resourceIdBytes))
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
	log, err := unmarshallToPartialStruct(logBytes)
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
