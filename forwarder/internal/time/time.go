// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package time

import (
	"time"
)

// Now is a function that returns the current time
type (
	Now func() time.Time
)
