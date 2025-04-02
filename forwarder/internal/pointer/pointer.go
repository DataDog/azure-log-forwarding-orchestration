// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package pointer

// Get returns a pointer to the value passed in.
func Get[T any](val T) *T {
	return &val
}
