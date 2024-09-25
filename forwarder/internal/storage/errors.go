package storage

import "fmt"

type NotFoundError struct {
	Item string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s not found", e.Item)
}
