package gospice

import "os"

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

const GO_SPICE_VERSION = "V6.0"
