package gospice

import (
	"fmt"
	"os"
	"runtime"
	"strings"
)

const GO_SPICE_VERSION = "9.1.0"

type ClientConfig struct {
	HttpUrl   string `json:"http_url,omitempty"`
	FlightUrl string `json:"flight_url,omitempty"`

	// httpUrlFromEnv and flightUrlFromEnv record that HttpUrl and FlightUrl
	// came from their environment variables rather than the built-in defaults.
	// A client on a custom Flight address keeps an explicitly configured Cloud
	// HTTP endpoint, and a Flight endpoint moved by the environment is only
	// paired with an HTTP endpoint the environment also named.
	httpUrlFromEnv   bool
	flightUrlFromEnv bool
}

// pairingConfigured reports whether HttpUrl belongs with FlightUrl: both are
// built-in defaults, or the HTTP endpoint was configured alongside an
// environment-supplied Flight endpoint.
func (c ClientConfig) pairingConfigured() bool {
	return !c.flightUrlFromEnv || c.httpUrlFromEnv
}

func LoadConfig() ClientConfig {
	return loadConfig("SPICE_HTTP_URL", "https://data.spiceai.io", "SPICE_FLIGHT_URL", "flight.spiceai.io:443")
}

func LoadLocalConfig() ClientConfig {
	return loadConfig("SPICE_LOCAL_HTTP_URL", "http://127.0.0.1:8090", "SPICE_LOCAL_FLIGHT_URL", "grpc://127.0.0.1:50051")
}

func loadConfig(httpEnv, httpDefault, flightEnv, flightDefault string) ClientConfig {
	_, httpUrlFromEnv := os.LookupEnv(httpEnv)
	_, flightUrlFromEnv := os.LookupEnv(flightEnv)
	return ClientConfig{
		HttpUrl:          getEnvOrDefault(httpEnv, httpDefault),
		FlightUrl:        getEnvOrDefault(flightEnv, flightDefault),
		httpUrlFromEnv:   httpUrlFromEnv,
		flightUrlFromEnv: flightUrlFromEnv,
	}
}

func getEnvOrDefault(key string, defaultValue string) string {
	if v, exists := os.LookupEnv(key); exists {
		return v
	}
	return defaultValue
}

func GetSpiceUserAgent() string {
	// get OS type, release and machine type
	// get Go version for SDK version

	osType := runtime.GOOS
	switch osType {
	case "darwin":
		osType = "Darwin"
	case "linux":
		osType = "Linux"
	case "windows":
		osType = "Windows"
	case "freebsd":
		osType = "FreeBSD"
	case "openbsd":
		osType = "OpenBSD"
	case "android":
		osType = "Android"
	case "ios":
		osType = "iOS"
	}

	osMachine := runtime.GOARCH
	switch osMachine {
	case "amd64":
		osMachine = "x86_64"
	case "386":
		osMachine = "i386"
	case "arm64":
		osMachine = "aarch64"
	}

	osVersion := GetOSRelease()

	userAgent := fmt.Sprintf("gospice/%s (%s/%s %s)", GO_SPICE_VERSION, osType, osVersion, osMachine)

	// strip any non-printable ASCII characters
	return RemoveNonPrintableASCII(userAgent)
}

func RemoveNonPrintableASCII(str string) string {
	var builder strings.Builder

	for _, ch := range str {
		if ch >= 32 && ch <= 126 { // printable ASCII characters range from 32 to 126
			builder.WriteRune(ch)
		}
	}

	return builder.String()
}
