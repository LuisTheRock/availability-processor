package health

import (
	"time"
)

type ComponentStatus struct {
	Status  string            `json:"status"`
	Details map[string]string `json:"details,omitempty"`
}

func Up() ComponentStatus {
	return ComponentStatus{Status: "UP"}
}

func Down() ComponentStatus {
	return ComponentStatus{Status: "DOWN", Details: map[string]string{"reason": "unreachable"}}
}

type Response struct {
	Status    string                     `json:"status"`
	Service   string                     `json:"service"`
	Checks    map[string]ComponentStatus `json:"checks,omitempty"`
	Timestamp time.Time                  `json:"timestamp"`
}

func NewHealthResponse(status string, checks map[string]ComponentStatus) *Response {
	return &Response{
		Status:    status,
		Service:   "service-default-settings",
		Checks:    checks,
		Timestamp: time.Now().UTC(),
	}
}

type DashboardResponse struct {
	Status         string                     `json:"status"`
	Service        string                     `json:"service"`
	LivenessState  string                     `json:"livenessState"`
	ReadinessState string                     `json:"readinessState"`
	Components     map[string]ComponentStatus `json:"components"`
	Timestamp      time.Time                  `json:"timestamp"`
}

func NewHealthDashboardResponse(status, livenessStatus string, checks map[string]ComponentStatus) *DashboardResponse {
	return &DashboardResponse{
		Status:         status,
		Service:        "service-default-settings",
		LivenessState:  livenessStatus,
		ReadinessState: status,
		Components:     checks,
		Timestamp:      time.Now().UTC(),
	}
}
