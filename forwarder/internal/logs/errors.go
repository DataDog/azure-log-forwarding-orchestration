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
