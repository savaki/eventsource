package eventsource

import (
	"encoding/json"
	"reflect"
)

type Serializer interface {
	Bind(events ...Event) error
	Serialize(event Event) (Record, error)
	Deserialize(record Record) (Event, error)
}

type jsonEvent struct {
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
}

type jsonSerializer struct {
	eventTypes map[string]reflect.Type
}

func (j *jsonSerializer) Bind(events ...Event) error {
	for _, event := range events {
		eventType, t := extractEventType(event)
		j.eventTypes[eventType] = t
	}

	return nil
}

func (j *jsonSerializer) Serialize(v Event) (Record, error) {
	eventType, _ := extractEventType(v)

	data, err := json.Marshal(v)
	if err != nil {
		return Record{}, err
	}

	data, err = json.Marshal(jsonEvent{
		Type: eventType,
		Data: json.RawMessage(data),
	})
	if err != nil {
		return Record{}, NewError(err, InvalidEncoding, "unable to encode event")
	}

	return Record{
		Version: v.EventVersion(),
		At:      Time(v.EventAt()),
		Data:    data,
	}, nil
}

func (j *jsonSerializer) Deserialize(record Record) (Event, error) {
	wrapper := jsonEvent{}
	err := json.Unmarshal(record.Data, &wrapper)
	if err != nil {
		return nil, NewError(err, InvalidEncoding, "unable to unmarshal event")
	}

	t, ok := j.eventTypes[wrapper.Type]
	if !ok {
		return nil, NewError(err, UnboundEventType, "unbound event type, %v", wrapper.Type)
	}

	v := reflect.New(t).Interface()
	err = json.Unmarshal(wrapper.Data, v)
	if err != nil {
		return nil, NewError(err, InvalidEncoding, "unable to unmarshal event data into %#v", v)
	}

	return v.(Event), nil
}

func JSONSerializer() Serializer {
	return &jsonSerializer{
		eventTypes: map[string]reflect.Type{},
	}
}
