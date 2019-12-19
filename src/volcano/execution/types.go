package execution

import (
	"encoding/json"
	"strconv"
	"time"

	"volcano/common"
)

type Datatype string

const (
	DatatypeBoolean Datatype = "boolean"
	DatatypeInt     Datatype = "int"
	DatatypeFloat64 Datatype = "float64"
	DatatypeString  Datatype = "string"
	DatatypeTuple   Datatype = "common.Tuple"
)

func GetType(i common.Value) Datatype {
	if _, ok := i.(common.Bool); ok {
		return DatatypeBoolean
	}
	if _, ok := i.(common.Int); ok {
		return DatatypeInt
	}
	if _, ok := i.(common.Float); ok {
		return DatatypeFloat64
	}
	if _, ok := i.(common.String); ok {
		return DatatypeString
	}
	if _, ok := i.(common.Tuple); ok {
		return DatatypeTuple
	}
	return DatatypeString // TODO: Unknown
}

// ParseType tries to parse the given string into any type it succeeds to. Returns back the string on failure.
func ParseType(str string) common.Value {
	integer, err := strconv.ParseInt(str, 10, 64)
	if err == nil {
		return common.MakeInt(int(integer))
	}

	float, err := strconv.ParseFloat(str, 64)
	if err == nil {
		return common.MakeFloat(float)
	}

	boolean, err := strconv.ParseBool(str)
	if err == nil {
		return common.MakeBool(boolean)
	}

	var jsonObject map[string]interface{}
	err = json.Unmarshal([]byte(str), &jsonObject)
	if err == nil {
		return common.NormalizeType(jsonObject)
	}

	t, err := time.Parse(time.RFC3339Nano, str)
	if err == nil {
		return common.MakeTime(t)
	}

	return common.MakeString(str)
}
