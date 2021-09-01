package hash_miner

import (
	"fmt"
	"testing"
)

func TestHasNumZeroesSuffix(t *testing.T) {
	type Test struct {
		str       string
		numZeroes uint
		expected  bool
	}
	tests := []Test{
		{"0", 2, false},
		{"0", 1, true},
		{"0", 0, true},
		{"0001", 2, false},
		{"0100", 3, false},
		{"01000", 3, true},
		{"01000", 1, true},
		{"3865626464376639316239323136366163333963666633386132373639303030", 3, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s, n = %d", test.str, test.numZeroes), func(t *testing.T) {
			result := hasNumZeroesSuffix([]byte(test.str), test.numZeroes)
			if test.expected != result {
				t.Fatalf("result %v should have been %v", result, test.expected)
			}
		})
	}
}
