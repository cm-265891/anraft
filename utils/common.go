package utils

import (
	"github.com/tsuru/config"
	"math/rand"
	"os"
	"time"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

func FileExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func Int64Min(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
