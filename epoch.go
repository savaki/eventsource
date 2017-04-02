package eventsource

import (
	"strconv"
	"time"
)

// EpochMillis represents the number of millis since Jan 1, 1970
type EpochMillis int64

// Int64 converts EpochMillis to int64 representation
func (e EpochMillis) Int64() int64 {
	return int64(e)
}

// String represents EpochMillis as a numeric string
func (e EpochMillis) String() string {
	return strconv.FormatInt(int64(e), 10)
}

// Time converts EpochMillis to an instance of time.Time
func (e EpochMillis) Time() time.Time {
	seconds := int64(e) / 1e3
	millis := int64(e) % 1e3
	return time.Unix(seconds, millis*1e6)
}

// Now is the current time in epoch millis; akin to time.Now()
func Now() EpochMillis {
	return Time(time.Now())
}

// Time converts a time.Time to an EpochMillis ; units of time less than millis are lost
func Time(t time.Time) EpochMillis {
	seconds := t.Unix() * 1e3
	millis := int64(t.Nanosecond()) / 1e6
	return EpochMillis(seconds + millis)
}
