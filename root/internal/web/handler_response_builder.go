package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

const (
	InternalServerError = "Internal Server Error"
)

type Response struct {
	StatusCode int `json:"-"`
	Msg        any `json:"msg"`
}

func (e *Response) Error() string {
	return fmt.Sprintf("api error: %v", e.StatusCode)
}

func NewResponse(statusCode int, msg any) *Response {
	return &Response{
		StatusCode: statusCode,
		Msg:        msg,
	}
}

type ApiFunc func(w http.ResponseWriter, r *http.Request) error

func Make(h ApiFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			if apiErr, ok := errors.AsType[*Response](err); ok {
				_ = WriteJSON(w, apiErr.StatusCode, apiErr)
			} else {
				_ = WriteJSON(w, http.StatusInternalServerError, struct {
					Msg string `json:"msg"`
				}{Msg: InternalServerError})
			}
		}
	}
}

func WriteJSON(w http.ResponseWriter, code int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	return json.NewEncoder(w).Encode(v)
}
