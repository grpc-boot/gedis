package gedis

import (
	"fmt"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/grpc-boot/base"
	"github.com/shopspring/decimal"
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
		var (
			val     = value.([]interface{})
			lngD, _ = decimal.NewFromString(base.Bytes2String(val[3].([]interface{})[0].([]byte)))
			latD, _ = decimal.NewFromString(base.Bytes2String(val[3].([]interface{})[1].([]byte)))
			loc     = Location{
				Member:   base.Bytes2String(val[0].([]byte)),
				Distance: base.Bytes2String(val[1].([]byte)),
				Hash:     val[2].(int64),
			}
		)

		loc.Lng, _ = lngD.Round(6).Float64()
		loc.Lat, _ = latD.Round(6).Float64()

		locationList = append(locationList, loc)
	}

	return locationList, nil
}

func Positions(reply interface{}, err error) ([]Position, error) {
	if err != nil {
		return nil, err
	}

	values, err := redigo.Values(reply, err)
	if err != nil {
		return nil, err
	}

	positionList := make([]Position, 0, len(values))

	for _, value := range values {
		var (
			val     = value.([]interface{})
			p       = Position{}
			lngD, _ = decimal.NewFromString(base.Bytes2String(val[0].([]byte)))
			latD, _ = decimal.NewFromString(base.Bytes2String(val[1].([]byte)))
		)
		p.Lng, _ = lngD.Round(6).Float64()
		p.Lat, _ = latD.Round(6).Float64()
		positionList = append(positionList, p)
	}

	return positionList, nil
}
