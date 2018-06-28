package utils

import (
	"github.com/tsuru/config"
)

func GetConfigIntOrDefault(key string, d int) int {
	r, err := config.GetInt(key)
	if err != nil {
		return d
	}
	return r
}

func GetConfigStringOrDefault(key string, d string) string {
	r, err := config.GetString(key)
	if err != nil {
		return d
	}
	return r
}
