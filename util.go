package tokyocabinet

import "C"
import (
	"math"
)

// simple wrapper for math.IsNaN for testing C.double returns
func isnan(n C.double) bool {
	return math.IsNaN(float64(n))
}
