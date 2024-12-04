package gospice

import (
	"regexp"
	"strings"
	"testing"
)

func TestUserAgent(t *testing.T) {
	userAgent := GetSpiceUserAgent()

	// test with a regex
	regex := regexp.MustCompile(`gospice/\d+\.\d+\.\d+ \((Linux|Windows|Darwin)/[\d\w\.\-\_]+ (x86_64|aarch64|i386)\)`)

	if !regex.MatchString(userAgent) {
		t.Errorf("User agent string is not in the expected format: %s", userAgent)
	}
}

func TestPrependedUserAgent(t *testing.T) {
	client := NewSpiceClient()
	client.Init(WithUserAgent("my-test-agent/1.0"))

	if !strings.HasPrefix(client.userAgent, "my-test-agent/1.0 gospice/") {
		t.Errorf("User agent string is not in the expected format: %s", client.userAgent)
	}
}
