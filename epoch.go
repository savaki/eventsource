package eventsource

import (
	"strconv"
	"time"
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
