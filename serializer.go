package eventsource

import (
	"encoding/json"
	"errors"
	"reflect"
)

type Serializer interface {
	Bind(...interface{}) error
	Serialize(interface{}) ([]byte, error)
	Deserialize(string, []byte) (interface{}, error)
}

type jsonSerializer struct {
	eventTypes map[string]reflect.Type
}

func (j *jsonSerializer) Bind(events ...interface{}) error {
	for _, event := range events {
		meta, err := Inspect(event)
		if err != nil {
			return err
		}

		t := reflect.TypeOf(event)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}

		j.eventTypes[meta.AggregateType] = t
	}

	return nil
}

func (j *jsonSerializer) Serialize(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j *jsonSerializer) Deserialize(eventType string, data []byte) (interface{}, error) {
	t, ok := j.eventTypes[eventType]
	if !ok {
		return nil, errors.New("unable to deserialize")
	}

	v := reflect.New(t).Interface()
	err := json.Unmarshal(data, v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func JSONSerializer() Serializer {
	return &jsonSerializer{
		eventTypes: map[string]reflect.Type{},
	}
}
