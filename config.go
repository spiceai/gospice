package gospice

import "runtime"
import "fmt"
import "os"
import "golang.org/x/sys/unix"

type ClientConfig struct {
	HttpUrl      string `json:"http_url,omitempty"`
	FlightUrl    string `json:"flight_url,omitempty"`
	FirecacheUrl string `json:"firecache_url,omitempty"`
}

func LoadConfig() ClientConfig {
	return ClientConfig{
		HttpUrl:      getEnvOrDefault("SPICE_HTTP_URL", "https://data.spiceai.io"),
		FirecacheUrl: getEnvOrDefault("SPICE_FIRECACHE_URL", "firecache.spiceai.io:443"),
		FlightUrl:    getEnvOrDefault("SPICE_FLIGHT_URL", "flight.spiceai.io:443"),
	}
}

func LoadLocalConfig() ClientConfig {
	return ClientConfig{
		HttpUrl:      getEnvOrDefault("SPICE_LOCAL_HTTP_URL", "http://localhost:3000"),
		FlightUrl:    getEnvOrDefault("SPICE_LOCAL_FLIGHT_URL", "grpc://localhost:50051"),
		FirecacheUrl: getEnvOrDefault("SPICE_FIRECACHE_URL", "firecache.spiceai.io:443"),
	}
}

func getEnvOrDefault(key string, defaultValue string) string {
	if v, exists := os.LookupEnv(key); exists {
		return v
	}
	return defaultValue
}

const GO_SPICE_VERSION = "v6.0"

func GetOSRelease() string {
	if runtime.GOOS == "windows" {
		// do the windows thing
		return "unknown"
	} else {
		u := unix.Utsname{}
		unix.Uname(&u)
		// convert []byte to string
		return string(u.Release[:])
	}
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

	return fmt.Sprintf("gospice %s (%s/%s %s)", GO_SPICE_VERSION, osType, osVersion, osMachine)
}
