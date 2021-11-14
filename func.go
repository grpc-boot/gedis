package gedis

import (
	"fmt"

	redigo "github.com/garyburd/redigo/redis"
)

func String(reply interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}

	switch rt := reply.(type) {
	case []byte:
		return string(rt), nil
	case string:
		return rt, nil
	case nil:
		return "", nil
	case redigo.Error:
		return "", rt
	}

	return "", fmt.Errorf("redigo: unexpected type for String, got type %T", reply)
}
