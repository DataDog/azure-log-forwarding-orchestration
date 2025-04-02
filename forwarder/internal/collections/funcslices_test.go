// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package collections_test

import (
	"fmt"
	"testing"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/collections"
	"github.com/stretchr/testify/assert"
)

func TestFilterMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []int
		mapFunc  func(int) (string, bool)
		expected []string
	}{
		{
			name:  "filter even numbers and convert to string",
			input: []int{1, 2, 3, 4, 5, 6},
			mapFunc: func(i int) (string, bool) {
				if i%2 == 0 {
					return fmt.Sprintf("even-%d", i), true
				}
				return "", false
			},
			expected: []string{"even-2", "even-4", "even-6"},
		},
		{
			name:  "filter numbers greater than 3 and convert to string",
			input: []int{1, 2, 3, 4, 5, 6},
			mapFunc: func(i int) (string, bool) {
				if i > 3 {
					return fmt.Sprintf("gt3-%d", i), true
				}
				return "", false
			},
			expected: []string{"gt3-4", "gt3-5", "gt3-6"},
		},
		{
			name:  "no elements match",
			input: []int{1, 2, 3},
			mapFunc: func(i int) (string, bool) {
				return "", false
			},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := collections.FilterMap(tt.input, tt.mapFunc)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []int
		filter   func(int) bool
		expected []int
	}{
		{
			name:  "filter even numbers",
			input: []int{1, 2, 3, 4, 5, 6},
			filter: func(i int) bool {
				return i%2 == 0
			},
			expected: []int{2, 4, 6},
		},
		{
			name:  "filter numbers greater than 3",
			input: []int{1, 2, 3, 4, 5, 6},
			filter: func(i int) bool {
				return i > 3
			},
			expected: []int{4, 5, 6},
		},
		{
			name:  "no elements match",
			input: []int{1, 2, 3},
			filter: func(i int) bool {
				return false
			},
			expected: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := collections.Filter(tt.input, tt.filter)
			assert.Equal(t, tt.expected, result)
		})
	}
}
