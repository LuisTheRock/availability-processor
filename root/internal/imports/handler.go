package imports

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"availability-processor/internal/availability"
	"availability-processor/internal/web"

	"go.uber.org/zap"
)

const (
	maxUploadBytes     = 10 << 20
	defaultErrorsLimit = 100
	maxErrorsLimit     = 1000
)

var requiredColumns = []string{"sellerId", "productId", "sizeId", "countryCode", "availableQuantity", "sourceVersion"}

// Row is one parsed CSV data row.
type Row struct {
	Num    int // 1-based, header excluded
	Change availability.Change
}

type Handler struct {
	svc *Service
	log *zap.Logger
}

func NewHandler(svc *Service, log *zap.Logger) *Handler { return &Handler{svc: svc, log: log} }

// HandleCreate accepts a CSV (raw text/csv body or multipart "file"). A bad
// header or unreadable file is a synchronous 400; malformed rows become row errors.
//
//	@Summary		Start an asynchronous CSV import
//	@Description	Columns: sellerId,productId,sizeId,countryCode,availableQuantity,sourceVersion. Send the CSV as a raw text/csv body, or as the multipart field "file" (10 MB maximum). Rows are processed in the background through the same flow as single events; poll the job for progress. Malformed rows are reported per row, never as a 400.
//	@Tags			imports
//	@Accept			mpfd
//	@Accept			text/csv
//	@Produce		json
//	@Param			file	formData	file	false	"CSV file (multipart upload)"
//	@Success		202		{object}	CreatedResponse
//	@Failure		400		{object}	web.Response	"missing or wrong header, empty file, or unreadable body"
//	@Failure		413		{object}	web.Response	"upload larger than 10 MB"
//	@Failure		503		{object}	web.Response	"job store unavailable"
//	@Router			/v1/availability/imports [post]
func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) error {
	// r.FormFile reads r.Body, so the cap must be set on r.Body itself.
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)

	var src io.Reader = r.Body
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		f, _, err := r.FormFile("file")
		if err != nil {
			return uploadError(err, `multipart field "file" is required`)
		}
		defer f.Close()
		src = f
	}

	rows, parseErrs, err := parseCSV(src)
	if err != nil {
		return uploadError(err, err.Error())
	}

	id, err := h.svc.Submit(r.Context(), rows, parseErrs)
	if err != nil {
		h.log.Error("creating import job", zap.Error(err))
		return web.NewResponse(http.StatusServiceUnavailable, "job store unavailable")
	}
	return web.WriteJSON(w, http.StatusAccepted, CreatedResponse{JobID: id})
}

// uploadError is 413 when the size cap was hit, else 400 with msg.
func uploadError(err error, msg string) *web.Response {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return web.NewResponse(http.StatusRequestEntityTooLarge, fmt.Sprintf("upload exceeds %d bytes", maxUploadBytes))
	}
	return web.NewResponse(http.StatusBadRequest, msg)
}

// CreatedResponse is the 202 body of an accepted upload.
type CreatedResponse struct {
	JobID string `json:"jobId"`
}

// StatusResponse is a job's counters plus a sample of its row errors.
type StatusResponse struct {
	Snapshot
	Errors []RowError `json:"errors"`
}

// HandleGet returns job progress from memory (running here) or Redis.
//
//	@Summary		Get the status of an import job
//	@Description	Counters are total, processed, succeeded, skipped (stale or duplicate versions) and failed. errors lists failed rows only, capped at 1000 per job. A job whose process died is reported as INTERRUPTED; resubmit the same CSV to finish it.
//	@Tags			imports
//	@Produce		json
//	@Param			jobId		path		string	true	"Job id returned by the upload"
//	@Param			errorsLimit	query		int		false	"Row errors to return: default 100, maximum 1000, 0 for none"
//	@Success		200			{object}	StatusResponse
//	@Failure		400			{object}	web.Response	"invalid errorsLimit"
//	@Failure		404			{object}	web.Response	"unknown job"
//	@Failure		503			{object}	web.Response	"job store unavailable"
//	@Router			/v1/availability/imports/{jobId} [get]
func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) error {
	limit := defaultErrorsLimit
	if v := r.URL.Query().Get("errorsLimit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return web.NewResponse(http.StatusBadRequest, "errorsLimit must be a non-negative integer")
		}
		limit = min(n, maxErrorsLimit)
	}

	sn, errs, err := h.svc.Get(r.Context(), r.PathValue("jobId"), limit)
	if errors.Is(err, ErrJobNotFound) {
		return web.NewResponse(http.StatusNotFound, "job not found")
	}
	if err != nil {
		h.log.Error("loading import job", zap.Error(err))
		return web.NewResponse(http.StatusServiceUnavailable, "job store unavailable")
	}
	return web.WriteJSON(w, http.StatusOK, StatusResponse{sn, errs})
}

func parseCSV(src io.Reader) (rows []Row, parseErrs []RowError, err error) {
	cr := csv.NewReader(src)
	cr.FieldsPerRecord = -1 // checked per row so one bad row does not end the read

	header, err := cr.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("reading CSV header: %w", err)
	}
	col := map[string]int{}
	for i, name := range header {
		col[strings.TrimSpace(strings.TrimPrefix(name, "\ufeff"))] = i
	}
	for _, name := range requiredColumns {
		if _, ok := col[name]; !ok {
			return nil, nil, fmt.Errorf("missing CSV column %q", name)
		}
	}

	for num := 1; ; num++ {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		var pe *csv.ParseError
		if errors.As(err, &pe) {
			parseErrs = append(parseErrs, RowError{num, "INVALID", err.Error()})
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("reading CSV: %w", err)
		}

		c, err := rowToChange(rec, col)
		if err != nil {
			parseErrs = append(parseErrs, RowError{num, "INVALID", err.Error()})
			continue
		}
		rows = append(rows, Row{num, c})
	}

	if len(rows)+len(parseErrs) == 0 {
		return nil, nil, errors.New("CSV has no data rows")
	}
	return rows, parseErrs, nil
}

func rowToChange(rec []string, col map[string]int) (availability.Change, error) {
	if len(rec) < len(requiredColumns) {
		return availability.Change{}, fmt.Errorf("expected %d columns, got %d", len(requiredColumns), len(rec))
	}
	get := func(name string) string { return strings.TrimSpace(rec[col[name]]) }

	qty, err := strconv.Atoi(get("availableQuantity"))
	if err != nil {
		return availability.Change{}, fmt.Errorf("availableQuantity: %w", err)
	}
	ver, err := strconv.ParseInt(get("sourceVersion"), 10, 64)
	if err != nil {
		return availability.Change{}, fmt.Errorf("sourceVersion: %w", err)
	}
	return availability.Change{
		SellerID: get("sellerId"), ProductID: get("productId"), SizeID: get("sizeId"),
		CountryCode: get("countryCode"), AvailableQuantity: qty, SourceVersion: ver,
	}, nil
}
