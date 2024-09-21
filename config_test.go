package gospice

import (
	"fmt"
	"testing"
)

func TestUserAgent(t *testing.T) {
	userAgent := GetSpiceUserAgent()

	// just print it
	fmt.Println(userAgent)
}
