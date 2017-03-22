package eventsource

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	tagName    = "eventsource"
	typePrefix = "type:"
)

type EpochMillis int64

func (e EpochMillis) Int64() int64 {
	return int64(e)
}

func (e EpochMillis) String() string {
	return strconv.FormatInt(int64(e), 10)
}

func (e EpochMillis) Time() time.Time {
	seconds := int64(e) / 1e3
	millis := int64(e) % 1e3
	return time.Unix(seconds, millis*1e6)
}

func Now() EpochMillis {
	return Time(time.Now())
}

func Time(t time.Time) EpochMillis {
	seconds := t.Unix() * 1e3
	millis := int64(t.Nanosecond()) / 1e6
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
	ID      string      `eventsource:"id"`
	Version int         `eventsource:"version"`
	At      EpochMillis `eventsource:"at"`
}

type inspector struct {
	HasID        bool
	HasEventType bool
	HasVersion   bool
	HasAt        bool
	ID           string
	EventType    string
	Version      int
	At           EpochMillis
}

func (gadget *inspector) inspect(event interface{}) error {
	if event == nil {
		return NewError(nil, AggregateNil, "Illegal attempt to inspect nil aggregate")
	}

	t := reflect.TypeOf(event)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	value := reflect.ValueOf(event)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := value.Field(i)

		// Check for embedded struct

		tag := field.Tag.Get(tagName)

		if tag == "" {
			switch field.Type.Kind() {
			case reflect.Struct, reflect.Interface:
				err := gadget.inspect(fieldValue.Interface())
				if err != nil {
					return err
				}
			}

			continue
		}

		if v := strings.Index(tag, ","); v > 0 {
			if v := tag[v+1:]; strings.HasPrefix(v, typePrefix) {
				gadget.EventType = v[len(typePrefix):]
				if gadget.HasEventType {
					return NewError(nil, DuplicateType, "duplicate type tag found")
				}
				gadget.HasEventType = true
			}
			tag = tag[0:v]
		}

		switch tag {
		case "id":
			if gadget.HasID {
				return NewError(nil, DuplicateID, "duplicate id tag found")
			}

			switch fieldValue := value.Field(i).Interface().(type) {
			case string:
				gadget.ID = fieldValue

			case fmt.Stringer:
				gadget.ID = fieldValue.String()

			default:
				return NewError(nil, InvalidID, "invalid type for id field; want string or fmt.String, got %#v", fieldValue)
			}
			gadget.HasID = true

		case "version":
			if gadget.HasVersion {
				return NewError(nil, DuplicateVersion, "duplicate version tag found")
			}
			switch fieldValue := value.Field(i).Interface().(type) {
			case int:
				gadget.Version = fieldValue
			default:
				return NewError(nil, InvalidVersion, "invalid type for version tag; want int, got %#v", fieldValue)
			}
			gadget.HasVersion = true

		case "at":
			if gadget.HasAt {
				return NewError(nil, DuplicateAt, "duplicate at tag found")
			}
			switch fieldValue := value.Field(i).Interface().(type) {
			case EpochMillis:
				gadget.At = fieldValue
			case int64:
				gadget.At = EpochMillis(fieldValue)
			case time.Time:
				gadget.At = Time(fieldValue)
			default:
				return NewError(nil, InvalidAt, "invalid at type for at tag; want be EpocMillis, int64, or time.Time, got %#v", fieldValue)
			}
			gadget.HasAt = true
		}
	}

	return nil
}

func Inspect(event interface{}) (EventMeta, error) {
	gadget := &inspector{}
	err := gadget.inspect(event)
	if err != nil {
		return EventMeta{}, err
	}

	meta := EventMeta{
		ID:        gadget.ID,
		EventType: gadget.EventType,
		Event:     event,
		Version:   gadget.Version,
		At:        gadget.At,
	}

	if meta.EventType == "" {
		t := reflect.TypeOf(event)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		meta.EventType = t.Name()
	}

	return meta, nil
}
