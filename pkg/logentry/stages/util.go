package stages

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	ErrTimestampContainsYear = "timestamp '%s' is expected to not contain the year date component"
)

// convertDateLayout converts pre-defined date format layout into date format
func convertDateLayout(predef string) parser {
	switch predef {
	case "ANSIC":
		return func(t string) (time.Time, error) {
			return time.Parse(time.ANSIC, t)
		}
	case "UnixDate":
		return func(t string) (time.Time, error) {
			return time.Parse(time.UnixDate, t)
		}
	case "RubyDate":
		return func(t string) (time.Time, error) {
			return time.Parse(time.RubyDate, t)
		}
	case "RFC822":
		return func(t string) (time.Time, error) {
			return time.Parse(time.RFC822, t)
		}
	case "RFC822Z":
		return func(t string) (time.Time, error) {
			return time.Parse(time.RFC822Z, t)
		}
	case "RFC850":
		return func(t string) (time.Time, error) {
			return time.Parse(time.RFC850, t)
		}
	case "RFC1123":
		return func(t string) (time.Time, error) {
			return time.Parse(time.RFC1123, t)
		}
	case "RFC1123Z":
		return func(t string) (time.Time, error) {
			return time.Parse(time.RFC1123Z, t)
		}
	case "RFC3339":
		return func(t string) (time.Time, error) {
			return time.Parse(time.RFC3339, t)
		}
	case "RFC3339Nano":
		return func(t string) (time.Time, error) {
			return time.Parse(time.RFC3339Nano, t)
		}
	case "Unix":
		return func(t string) (time.Time, error) {
			i, err := strconv.ParseInt(t, 10, 64)
			if err != nil {
				return time.Time{}, err
			}
			return time.Unix(i, 0), nil
		}
	case "UnixMs":
		return func(t string) (time.Time, error) {
			i, err := strconv.ParseInt(t, 10, 64)
			if err != nil {
				return time.Time{}, err
			}
			return time.Unix(0, i*int64(time.Millisecond)), nil
		}
	case "UnixNs":
		return func(t string) (time.Time, error) {
			i, err := strconv.ParseInt(t, 10, 64)
			if err != nil {
				return time.Time{}, err
			}
			return time.Unix(0, i), nil
		}
	default:
		if !strings.Contains(predef, "2006") {
			return func(t string) (time.Time, error) {
				return parseTimestampWithoutYear(predef, t, time.Now())
			}
		}
		return func(t string) (time.Time, error) {
			return time.Parse(predef, t)
		}
	}
}

// parseTimestampWithoutYear parses the input timestamp without the year component,
// assuming the timestamp is related to a point in time close to "now", and correctly
// handling the edge cases around new year's eve
func parseTimestampWithoutYear(layout string, timestamp string, now time.Time) (time.Time, error) {
	parsedTime, err := time.Parse(layout, timestamp)
	if err != nil {
		return parsedTime, err
	}

	// Ensure the year component of the input date string has not been
	// parsed for real
	if parsedTime.Year() != 0 {
		return parsedTime, fmt.Errorf(ErrTimestampContainsYear, timestamp)
	}

	// Handle the case we're crossing the new year's eve midnight
	if parsedTime.Month() == 12 && now.Month() == 1 {
		parsedTime = parsedTime.AddDate(now.Year()-1, 0, 0)
	} else if parsedTime.Month() == 1 && now.Month() == 12 {
		parsedTime = parsedTime.AddDate(now.Year()+1, 0, 0)
	} else {
		parsedTime = parsedTime.AddDate(now.Year(), 0, 0)
	}

	return parsedTime, nil
}

// getString will convert the input variable to a string if possible
func getString(unk interface{}) (string, error) {

	switch i := unk.(type) {
	case float64:
		return strconv.FormatFloat(i, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(i), 'f', -1, 32), nil
	case int64:
		return strconv.FormatInt(i, 10), nil
	case int32:
		return strconv.FormatInt(int64(i), 10), nil
	case int:
		return strconv.Itoa(i), nil
	case uint64:
		return strconv.FormatUint(i, 10), nil
	case uint32:
		return strconv.FormatUint(uint64(i), 10), nil
	case uint:
		return strconv.FormatUint(uint64(i), 10), nil
	case string:
		return unk.(string), nil
	case bool:
		if i {
			return "true", nil
		}
		return "false", nil
	default:
		return "", fmt.Errorf("Can't convert %v to string", unk)
	}
}
