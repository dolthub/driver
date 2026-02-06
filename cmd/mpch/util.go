package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"syscall"
)

func newRunID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "run-" + hex.EncodeToString(b[:]), nil
}

func isBrokenPipe(err error) bool {
	// We care about the common case: write to a closed pipe.
	return errors.Is(err, syscall.EPIPE)
}
