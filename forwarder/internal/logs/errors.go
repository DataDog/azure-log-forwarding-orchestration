// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs

import "errors"

var (
	// ErrIncompleteLogFile is an error for when a log file is incomplete.
	ErrIncompleteLogFile = errors.New("received a partial log file")

	// ErrInvalidLog is an error for when a log is invalid.
	ErrInvalidLog = errors.New("invalid log")

	// ErrUnexpectedToken is an error for when an unexpected token is found in a log.
	ErrUnexpectedToken = errors.New("found unexpected token in log")
)
