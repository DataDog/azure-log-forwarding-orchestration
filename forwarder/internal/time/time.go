package time

import (
	"os"
	"path"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Record struct {
	Now time.Time `yaml:"now"`
}

type (
	Now func() time.Time
)

func getRecordPath() string {
	pathRoot, err := os.Getwd()
	if err != nil {
		log.Fatalf("getwd err   #%v ", err)
	}
	if !strings.Contains(pathRoot, "cmd/forwarder") {
		pathRoot = path.Join(pathRoot, "cmd", "forwarder")
	}
	return path.Join(pathRoot, "fixtures", "time.yaml")
}

func RecordedNow() time.Time {
	file, err := os.ReadFile(getRecordPath())
	if err != nil {
		log.Fatalf("file read err   #%v ", err)
	}
	var r = &Record{}
	err = yaml.Unmarshal(file, r)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return r.Now
}

func RecordNow() error {
	file, err := os.OpenFile(getRecordPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := yaml.NewEncoder(file)

	return enc.Encode(Record{
		Now: time.Now(),
	})
}
