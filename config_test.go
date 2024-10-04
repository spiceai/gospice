package gospice

import (
	"regexp"
	"testing"
)

func TestUserAgent(t *testing.T) {
	userAgent := GetSpiceUserAgent()

	// test with a regex
	regex := regexp.MustCompile(`gospice \d+\.\d+\.\d+ \((Linux|Windows|Darwin)/[\d\w\.\-\_]+ (x86_64|aarch64|i386)\)`)

	if !regex.MatchString(userAgent) {
		t.Errorf("User agent string is not in the expected format: %s", userAgent)
	}
}
