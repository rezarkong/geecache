package logx

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
)

// Event prints one structured log line using stable key ordering so logs stay
// easy to grep and diff without introducing a heavier logging dependency.
func Event(component, action string, fields map[string]interface{}) {
	parts := []string{
		"component=" + formatValue(component),
		"event=" + formatValue(action),
	}

	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		parts = append(parts, key+"="+formatValue(fields[key]))
	}

	log.Print(strings.Join(parts, " "))
}

func formatValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(v)
	case []string:
		if len(v) == 0 {
			return "[]"
		}
		items := make([]string, 0, len(v))
		for _, item := range v {
			items = append(items, strconv.Quote(item))
		}
		return "[" + strings.Join(items, ",") + "]"
	case fmt.Stringer:
		return strconv.Quote(v.String())
	case error:
		return strconv.Quote(v.Error())
	default:
		return fmt.Sprint(v)
	}
}
