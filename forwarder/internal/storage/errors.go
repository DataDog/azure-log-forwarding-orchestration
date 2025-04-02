// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package storage

// NotFoundError is returned when an item is not found
type NotFoundError struct {
	Item string
}

// Error returns what item was not found
func (e *NotFoundError) Error() string {
	return e.Item + " not found"
}
