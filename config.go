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

func getEnvOrDefault(key string, defaultValue string) string {
	if v, exists := os.LookupEnv(key); exists {
		return v
	}
	return defaultValue
}
