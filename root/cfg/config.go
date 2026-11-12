package cfg

import (
	"cmp"
	"os"

	"github.com/joho/godotenv"
)

type ApiConfig struct {
	Server       serverConfig
	Dependencies dependenciesConfig
	Redis        redisConfig
}

type serverConfig struct {
	Address string
}

type dependenciesConfig struct {
	BaseURL string
}

type redisConfig struct {
	Addr string
}

func NewApiConfig() (*ApiConfig, error) {
	_ = godotenv.Load()

	return &ApiConfig{
		Server:       serverConfig{Address: cmp.Or(os.Getenv("SERVER_PORT"), ":5050")},
		Dependencies: dependenciesConfig{BaseURL: cmp.Or(os.Getenv("DEPENDENCIES_BASE_URL"), "http://localhost:8080")},
		Redis:        redisConfig{Addr: cmp.Or(os.Getenv("REDIS_ADDR"), "localhost:6379")},
	}, nil
}
