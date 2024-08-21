package logs

import (
	"bytes"
	"encoding/json"
)

type Log struct {
	ByteSize      int
	ForwarderName string
	//DDRequire     DDLogs
	Rest json.RawMessage `json:"-"`
}

//type DDLogs struct {
//	ResourceId       string `json:"resourceId"` // important
//	Category         string `json:"category"`
//	DDSource         string `json:"ddsource"`
//	DDSourceCategory string `json:"ddsourcecategory"`
//	Service          string `json:"service"`
//	DDTags           string `json:"ddtags"` // string array of tags
//}

func unmarshallToPartialStruct(azureLog []byte) (*Log, error) {
	var err error
	// partially unmarshall json to struct and keep remaining data as Raw json
	var logStruct Log
	tempJson := make(map[string]json.RawMessage)
	if err = json.Unmarshal(azureLog, &tempJson); err != nil {
		return nil, err
	}

	//err = b.getAzureLogFieldsFromJson(&azureStruct, tempJson)
	//azureStruct.Rest, _ = json.Marshal(tempJson)
	return &logStruct, err
}

func Format(logBytes []byte) ([]byte, error) {
	logBytes = bytes.ReplaceAll(logBytes, []byte("'"), []byte("\""))
	_, err := unmarshallToPartialStruct(logBytes)
	if err != nil {
		return nil, err
	}
	return logBytes, nil
}
