package config

import (
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ServiceToken string
	HTTPAddress  string
	EntitiesURL  string
}

func Load() (Config, error) {
	c := Config{
		ServiceToken: os.Getenv("RECOMMENDATIONS_SERVICE_TOKEN"),
		EntitiesURL:  strings.TrimSpace(os.Getenv("ENTITIES_URL")),
	}
	if len(c.ServiceToken) < 32 || len(c.ServiceToken) > 4096 || strings.ContainsAny(c.ServiceToken, " \t\r\n") {
		return Config{}, errors.New("RECOMMENDATIONS_SERVICE_TOKEN must contain 32–4096 non-whitespace bytes")
	}
	c.HTTPAddress = strings.TrimSpace(os.Getenv("HTTP_ADDRESS"))
	if c.HTTPAddress == "" {
		c.HTTPAddress = strings.TrimSpace(os.Getenv("HTTP_ADDR"))
	}
	if c.HTTPAddress == "" {
		if port := strings.TrimSpace(os.Getenv("PORT")); port != "" {
			c.HTTPAddress = ":" + port
		} else {
			c.HTTPAddress = ":8080"
		}
	}
	_, port, err := net.SplitHostPort(c.HTTPAddress)
	if err != nil {
		return Config{}, errors.New("HTTP address must be host:port")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return Config{}, errors.New("HTTP port must be between 1 and 65535")
	}

	return c, nil
}
