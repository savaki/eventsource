package eventsource

import (
	"encoding/json"
	"reflect"
)

type Serializer interface {
	Bind(events ...interface{}) error
	Serialize(event interface{}) (Record, error)
	Deserialize(record Record) (interface{}, error)
}

type jsonEvent struct {
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
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

		j.eventTypes[meta.EventType] = t
	}

	return nil
}

func (j *jsonSerializer) Serialize(v interface{}) (Record, error) {
	meta, err := Inspect(v)
	if err != nil {
		return Record{}, err
	}

	data, err := json.Marshal(v)
	if err != nil {
		return Record{}, err
	}

	data, err = json.Marshal(jsonEvent{
		Type: meta.EventType,
		Data: json.RawMessage(data),
	})
	if err != nil {
		return Record{}, NewError(err, InvalidEncoding, "unable to encode event")
	}

	return Record{
		Version: meta.Version,
		At:      meta.At,
		Data:    data,
	}, nil
}

func (j *jsonSerializer) Deserialize(record Record) (interface{}, error) {
	event := jsonEvent{}
	err := json.Unmarshal(record.Data, &event)
	if err != nil {
		return nil, NewError(err, InvalidEncoding, "unable to unmarshal event")
	}

	t, ok := j.eventTypes[event.Type]
	if !ok {
		return nil, NewError(err, UnboundEventType, "unbound event type, %v", event.Type)
	}

	v := reflect.New(t).Interface()
	err = json.Unmarshal(event.Data, v)
	if err != nil {
		return nil, NewError(err, InvalidEncoding, "unable to unmarshal event data into %#v", v)
	}

	return v, nil
}

func JSONSerializer() Serializer {
	return &jsonSerializer{
		eventTypes: map[string]reflect.Type{},
	}
}
