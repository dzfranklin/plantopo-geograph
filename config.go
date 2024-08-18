package geograph

import "os"

func GetEnvString(k string) string {
	v := os.Getenv(k)
	if v == "" {
		panic("Missing environment variable " + k)
	}
	return v
}
