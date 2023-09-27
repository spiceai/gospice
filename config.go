package gospice

import "os"

type ClientConfig struct {
	HttpUrl      string `json:"http_url,omitempty"`
	FlightUrl    string `json:"flight_url,omitempty"`
	FirecacheUrl string `json:"firecache_url,omitempty"`
}

var DEFAULT_CLIENT_CONFIG = ClientConfig{
	HttpUrl:      "https://data.spiceai.io",
	FlightUrl:    "flight.spiceai.io:443",
	FirecacheUrl: "firecache.spiceai.io:443",
}

func LoadConfig() ClientConfig {
	base := DEFAULT_CLIENT_CONFIG

	// Env variables
	if v, exists := os.LookupEnv("SPICE_HTTP_URL"); exists {
		base.HttpUrl = v
	}
	if v, exists := os.LookupEnv("SPICE_FIRECACHE_URL"); exists {
		base.FirecacheUrl = v
	}
	if v, exists := os.LookupEnv("SPICE_FLIGHT_URL"); exists {
		base.FlightUrl = v
	}

	return base
}
