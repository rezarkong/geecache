package registry

import (
	"encoding/json"
	"strings"
)

// ServiceRecord is stored in etcd as the service registration payload.
type ServiceRecord struct {
	Addr   string `json:"addr"`
	Weight int    `json:"weight,omitempty"`
}

func (r ServiceRecord) Normalize() ServiceRecord {
	r.Addr = strings.TrimSpace(r.Addr)
	if r.Weight <= 0 {
		r.Weight = 1
	}
	return r
}

func EncodeRecord(addr string, weight int) (string, error) {
	record := ServiceRecord{Addr: addr, Weight: weight}.Normalize()
	body, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func DecodeRecord(raw string) (ServiceRecord, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ServiceRecord{}, false
	}
	var record ServiceRecord
	if err := json.Unmarshal([]byte(raw), &record); err == nil && strings.TrimSpace(record.Addr) != "" {
		return record.Normalize(), true
	}
	return ServiceRecord{Addr: raw, Weight: 1}, true
}
