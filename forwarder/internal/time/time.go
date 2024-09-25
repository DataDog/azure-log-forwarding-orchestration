package time

import (
	"time"
)

// Now is a function that returns the current time
type (
	Now func() time.Time
)
