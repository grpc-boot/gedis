package gedis

import (
	"fmt"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/grpc-boot/base"
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

func Locations(reply interface{}, err error) ([]Location, error) {
	if err != nil {
		return nil, err
	}

	values, err := redigo.Values(reply, err)
	if err != nil {
		return nil, err
	}

	locationList := make([]Location, 0, len(values))

	for _, value := range values {
		val := value.([]interface{})
		locationList = append(locationList, Location{
			Member:   base.Bytes2String(val[0].([]byte)),
			Distance: base.Bytes2String(val[1].([]byte)),
			Hash:     val[2].(int64),
			Lng:      base.Bytes2Float64(val[3].([]interface{})[0].([]byte)),
			Lat:      base.Bytes2Float64(val[3].([]interface{})[1].([]byte)),
		})
	}

	return locationList, nil
}
