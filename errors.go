package tokyocabinet

import (
	"fmt"
)

type TokyoCabinetError struct {
	code int
	msg  string
}

func NewTokyoCabinetError(code int, msg string) *TokyoCabinetError {
	return &TokyoCabinetError{code, msg}
}

func (e TokyoCabinetError) Error() string {
	return fmt.Sprintf("TokyoCabinet error (%q) %q", string(e.code), string(e.msg))
}
