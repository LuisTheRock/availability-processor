package availability

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"availability-processor/internal/upstream"
	"availability-processor/internal/web"

	"go.uber.org/zap"
)

const maxEventBytes = 1 << 20

type Handler struct {
	proc *Processor
	log  *zap.Logger
}

func NewHandler(proc *Processor, log *zap.Logger) *Handler {
	return &Handler{proc: proc, log: log}
}

// HandleMessage is the test-only ingress: it calls the same Process a broker
// consumer adapter would.
//
//	@Summary		Apply one inventory change (test-only ingress)
//	@Description	Runs the flow a broker consumer would run: validate, version check, product context, availability write, output event. availableQuantity and sourceVersion are required and unknown fields are rejected. A stale or duplicate version is skipped without calling any dependency.
//	@Tags			availability
//	@Accept			json
//	@Produce		json
//	@Param			event	body		Change	true	"SellerInventoryChanged event"
//	@Success		200		{object}	Result
//	@Failure		400		{object}	web.Response	"invalid event"
//	@Failure		422		{object}	web.Response	"unknown product, inactive product, or country not enabled"
//	@Failure		502		{object}	web.Response	"a dependency failed after retries, or the output event could not be published"
//	@Failure		503		{object}	web.Response	"stock key busy, or a dependency is throttling (see the Retry-After header)"
//	@Router			/_dev/messages [post]
func (h *Handler) HandleMessage(w http.ResponseWriter, r *http.Request) error {
	// Quantity and version are pointers so a missing field is a 400, not a
	// silent 0 (which would mark the size sold out); typos are rejected too.
	var in struct {
		Change
		AvailableQuantity *int   `json:"availableQuantity"`
		SourceVersion     *int64 `json:"sourceVersion"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEventBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return web.NewResponse(http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	if in.AvailableQuantity == nil || in.SourceVersion == nil {
		return web.NewResponse(http.StatusBadRequest, "availableQuantity and sourceVersion are required")
	}
	c := in.Change
	c.AvailableQuantity, c.SourceVersion = *in.AvailableQuantity, *in.SourceVersion
	if c.EventType != "SellerInventoryChanged" || c.SchemaVersion != 1 {
		return web.NewResponse(http.StatusBadRequest,
			fmt.Sprintf("unsupported eventType/schemaVersion: %q/%d", c.EventType, c.SchemaVersion))
	}

	res, err := h.proc.ProcessEvent(r.Context(), c)
	if err != nil {
		return h.toResponse(w, c, err)
	}
	return web.WriteJSON(w, http.StatusOK, res)
}

func (h *Handler) toResponse(w http.ResponseWriter, c Change, err error) error {
	var rl *upstream.RateLimitedError
	switch {
	case errors.As(err, &rl): // a dependency is throttling: tell the caller when to retry
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(rl.RetryAfter.Seconds())))))
		return web.NewResponse(http.StatusServiceUnavailable, Code(err))
	case errors.Is(err, ErrInvalid):
		return web.NewResponse(http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrUnknownProduct), errors.Is(err, ErrInactiveProduct), errors.Is(err, ErrCountryDisabled):
		return web.NewResponse(http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrBusy):
		return web.NewResponse(http.StatusServiceUnavailable, err.Error())
	default: // dependency failure after retries, or a failed publish
		h.log.Error("processing event", zap.String("eventId", c.EventID), zap.Error(err))
		return web.NewResponse(http.StatusBadGateway, Code(err))
	}
}
