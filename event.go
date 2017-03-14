package eventsource

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

const (
	tagName    = "eventsource"
	typePrefix = "type:"
)

var (
	modelType reflect.Type
	idField   int
)

func init() {
	modelType = reflect.TypeOf(Model{})

	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		if field.Name == "AggregateID" {
			idField = i
			break
		}
	}
}

type EpochMillis int64

func (e EpochMillis) Time() time.Time {
	seconds := int64(e) / 1e3
	millis := int64(e) % 1e3
	return time.Unix(seconds, millis*1e6)
}

func Now() EpochMillis {
	now := time.Now()
	seconds := now.Unix() * 1e3
	millis := int64(now.Nanosecond()) / 1e6
	return EpochMillis(seconds + millis)
}

type EventMeta struct {
	ID        string
	EventType string
	Event     interface{}
	Version   int
	At        EpochMillis
}

type Model struct {
	ID      string
	Version int
	At      EpochMillis
}

func Inspect(event interface{}) (EventMeta, error) {
	meta := EventMeta{
		Event: event,
	}

	if event == nil {
		return meta, errors.New("cannot inspect nil")
	}

	eventType := reflect.TypeOf(event)
	if eventType.Kind() == reflect.Ptr {
		eventType = eventType.Elem()
	}

	eventValue := reflect.ValueOf(event)
	if eventValue.Kind() == reflect.Ptr {
		eventValue = eventValue.Elem()
	}

	hasID := false
	hasEventType := false
	hasVersion := false
	hasAt := false

	for i := 0; i < eventType.NumField(); i++ {
		field := eventType.Field(i)

		// Check for embedded Model

		if field.Name == "Model" && field.Type == modelType {
			if m, ok := eventValue.Field(i).Interface().(Model); ok {
				meta.ID = m.ID
				meta.Version = m.Version
				meta.At = m.At

				hasID = true
				hasVersion = true
				hasAt = true

				continue
			}
		}

		tag := field.Tag.Get(tagName)
		if tag == "" {
			continue
		}

		if v := strings.Index(tag, ","); v > 0 {
			if v := tag[v+1:]; strings.HasPrefix(v, typePrefix) {
				meta.EventType = v[len(typePrefix):]
				hasEventType = true
			}
			tag = tag[0:v]
		}

		switch tag {
		case "id":
			if hasID {
				return meta, errors.New("duplicate defintion of id found")
			}
			switch fieldValue := eventValue.Field(i).Interface().(type) {
			case string:
				meta.ID = fieldValue
			case fmt.Stringer:
				meta.ID = fieldValue.String()
			default:
				return meta, errors.New("eventsource id field must be either string or fmt.Stringer")
			}
			hasID = true

		case "version":
			if hasVersion {
				return meta, errors.New("duplicate defintion of version found")
			}
			switch fieldValue := eventValue.Field(i).Interface().(type) {
			case int:
				meta.Version = fieldValue
			default:
				return meta, errors.New("eventsource version field must be of type int")
			}
			hasAt = hasVersion

		case "at":
			if hasAt {
				return meta, errors.New("duplicate defintion of at found")
			}
			switch fieldValue := eventValue.Field(i).Interface().(type) {
			case EpochMillis:
				meta.At = fieldValue
			case int64:
				meta.At = EpochMillis(fieldValue)
			case time.Time:
				meta.At = Time(fieldValue)
			default:
				return meta, errors.New("eventsource version field must be of type int64 or time.Time")
			}
			hasAt = true
		}
	}

	if !hasEventType {
		meta.EventType = eventType.Name()
	}

	return meta, nil
}

func Time(t time.Time) EpochMillis {
	seconds := t.Unix()
	millis := int64(t.Nanosecond()) / 1e6
	return EpochMillis(seconds*1e3 + millis)
}

func setAggregateID(aggregate interface{}, aggregateID string) error {
	t := reflect.TypeOf(aggregate)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	eventValue := reflect.ValueOf(aggregate)
	if eventValue.Kind() == reflect.Ptr {
		eventValue = eventValue.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Check for embedded Model

		if field.Name == "Model" && field.Type == modelType {
			idField := eventValue.Field(i).Field(idField)
			if idField.CanSet() {
				idField.SetString(aggregateID)
				return nil
			}
		}

		// Otherwise, look for tagged field

		tag := field.Tag.Get(tagName)
		if tag == "" {
			continue
		}

		if v := strings.Index(tag, ","); v > 0 {
			tag = tag[0:v]
		}

		if tag != "id" {
			continue
		}

		idField := eventValue.Field(i)
		if idField.CanSet() {
			idField.SetString(aggregateID)
			return nil
		}
	}

	return errors.New("unable to set id")
}
