// Package availability is the shared business flow used by both the single
// event endpoint and the bulk import worker:
// validate -> version check -> enrich -> availability write -> publish.
package availability

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"

	"availability-processor/internal/upstream"
)

var (
	ErrInvalid         = errors.New("invalid change")
	ErrUnknownProduct  = errors.New("unknown product")
	ErrInactiveProduct = errors.New("inactive product")
	ErrCountryDisabled = errors.New("country not enabled for product")
	ErrPublish         = errors.New("publishing output event")
	ErrBusy            = errors.New("stock key is busy")
)

const (
	StatusApplied = "applied"
	StatusSkipped = "skipped"

	ReasonStale     = "STALE"
	ReasonDuplicate = "DUPLICATE"
)

// Change is an inbound SellerInventoryChanged event or one CSV row.
type Change struct {
	EventID           string    `json:"eventId"`
	EventType         string    `json:"eventType"`
	SchemaVersion     int       `json:"schemaVersion"`
	OccurredAt        time.Time `json:"occurredAt"`
	SellerID          string    `json:"sellerId"`
	ProductID         string    `json:"productId"`
	SizeID            string    `json:"sizeId"`
	CountryCode       string    `json:"countryCode"`
	AvailableQuantity int       `json:"availableQuantity"`
	SourceVersion     int64     `json:"sourceVersion"`
}

// Key identifies one stock stream; sourceVersion is only comparable within it.
type Key struct {
	Seller, Product, Size, Country string
}

func (k Key) String() string {
	return k.Seller + "|" + k.Product + "|" + k.Size + "|" + k.Country
}

func (c Change) Key() Key {
	return Key{c.SellerID, c.ProductID, c.SizeID, c.CountryCode}
}

var countryRe = regexp.MustCompile(`^[A-Z]{2}$`)

func (c Change) Validate() error {
	// A slice, not a map: map order is random, and the message would name a
	// different missing field from run to run.
	for _, f := range [...]struct{ name, value string }{
		{"sellerId", c.SellerID}, {"productId", c.ProductID}, {"sizeId", c.SizeID},
	} {
		if f.value == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalid, f.name)
		}
	}
	if !countryRe.MatchString(c.CountryCode) {
		return fmt.Errorf("%w: countryCode must be two uppercase letters", ErrInvalid)
	}
	if c.AvailableQuantity < 0 {
		return fmt.Errorf("%w: availableQuantity must be >= 0", ErrInvalid)
	}
	if c.SourceVersion < 0 {
		return fmt.Errorf("%w: sourceVersion must be >= 0", ErrInvalid)
	}
	return nil
}

type Result struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// Lookup resolves the product context for a change: one HTTP call for a
// single event, a snapshot map lookup for bulk rows.
type Lookup func(ctx context.Context, c Change) (upstream.ProductContext, error)

type VersionStore interface {
	// Lock serialises work on one stock key across goroutines and pods.
	Lock(ctx context.Context, k Key) (unlock func(), err error)
	Applied(ctx context.Context, k Key) (version int64, ok bool, err error)
	// Record stores v unless an equal or newer version is already recorded.
	Record(ctx context.Context, k Key, v int64) (recorded bool, err error)
}

type Processor struct {
	client   *upstream.Client
	versions VersionStore
}

func NewProcessor(client *upstream.Client, versions VersionStore) *Processor {
	return &Processor{client: client, versions: versions}
}

// ProcessEvent applies a single change, looking its context up over HTTP.
func (p *Processor) ProcessEvent(ctx context.Context, c Change) (Result, error) {
	ctx = upstream.WithFailFast429(ctx) // a 429 goes back to the caller; no sleeping while holding the key lock
	return p.Process(ctx, c, func(ctx context.Context, c Change) (upstream.ProductContext, error) {
		return p.client.ProductContext(ctx, c.ProductID, c.SizeID, c.CountryCode)
	})
}

type productContextBody struct {
	ProductName string         `json:"productName"`
	Brand       upstream.Named `json:"brand"`
	SizeLabel   string         `json:"sizeLabel"`
	SizeSystem  string         `json:"sizeSystem"`
}

type availabilityBody struct {
	AvailableQuantity int                `json:"availableQuantity"`
	SourceVersion     int64              `json:"sourceVersion"`
	ProductContext    productContextBody `json:"productContext"`
}

type sizeAvailabilityUpdated struct {
	EventID           string             `json:"eventId"`
	EventType         string             `json:"eventType"`
	SchemaVersion     int                `json:"schemaVersion"`
	OccurredAt        time.Time          `json:"occurredAt"`
	SellerID          string             `json:"sellerId"`
	ProductID         string             `json:"productId"`
	SizeID            string             `json:"sizeId"`
	CountryCode       string             `json:"countryCode"`
	AvailableQuantity int                `json:"availableQuantity"`
	SourceVersion     int64              `json:"sourceVersion"`
	ProductContext    productContextBody `json:"productContext"`
}

// Process runs the shared flow for one change. Stale and duplicate versions
// are not errors: they return a skipped Result and touch no dependency.
func (p *Processor) Process(ctx context.Context, c Change, lookup Lookup) (Result, error) {
	if err := c.Validate(); err != nil {
		return Result{}, err
	}

	k := c.Key()
	unlock, err := p.versions.Lock(ctx, k)
	if err != nil {
		return Result{}, err
	}
	defer unlock()

	// The row must finish before the lock lease can expire, or another pod
	// could write a newer version and this row would overwrite it.
	ctx, cancel := context.WithTimeout(ctx, lockLease-5*time.Second)
	defer cancel()

	applied, ok, err := p.versions.Applied(ctx, k)
	if err != nil {
		return Result{}, fmt.Errorf("reading applied version: %w", err)
	}
	if ok && c.SourceVersion < applied {
		return Result{StatusSkipped, ReasonStale}, nil
	}
	if ok && c.SourceVersion == applied {
		return Result{StatusSkipped, ReasonDuplicate}, nil
	}

	pc, err := lookup(ctx, c)
	if errors.Is(err, upstream.ErrNotFound) {
		return Result{}, fmt.Errorf("%w: %s/%s/%s", ErrUnknownProduct, c.ProductID, c.SizeID, c.CountryCode)
	}
	if err != nil {
		return Result{}, err
	}
	if pc.ProductStatus != "ACTIVE" {
		return Result{}, fmt.Errorf("%w: %s is %s", ErrInactiveProduct, c.ProductID, pc.ProductStatus)
	}
	if !pc.CountryEnabled {
		return Result{}, fmt.Errorf("%w: %s in %s", ErrCountryDisabled, c.ProductID, c.CountryCode)
	}

	body := productContextBody{pc.ProductName, pc.Brand, pc.SizeLabel, pc.SizeSystem}
	err = p.client.PutAvailability(ctx, c.SellerID, c.ProductID, c.SizeID, c.CountryCode,
		availabilityBody{c.AvailableQuantity, c.SourceVersion, body})
	if err != nil {
		return Result{}, err
	}

	// The write is done: record it before publishing so a failed publish is
	// never re-applied (see README: outbox is the production fix).
	recorded, err := p.versions.Record(ctx, k, c.SourceVersion)
	if err != nil {
		return Result{}, fmt.Errorf("recording version: %w", err)
	}
	if !recorded { // a newer version won while we were writing: do not announce ours
		return Result{StatusSkipped, ReasonStale}, nil
	}

	eventID := cmp.Or(c.EventID, outputEventID(k, c.SourceVersion))

	err = p.client.Publish(ctx, sizeAvailabilityUpdated{
		EventID:           eventID,
		EventType:         "SizeAvailabilityUpdated",
		SchemaVersion:     1,
		OccurredAt:        time.Now().UTC(),
		SellerID:          c.SellerID,
		ProductID:         c.ProductID,
		SizeID:            c.SizeID,
		CountryCode:       c.CountryCode,
		AvailableQuantity: c.AvailableQuantity,
		SourceVersion:     c.SourceVersion,
		ProductContext:    body,
	})
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrPublish, err)
	}
	return Result{Status: StatusApplied}, nil
}

// outputEventID names the output event of a CSV row (no inbound eventId).
// Events reuse their inbound eventId, so a delivery can be traced end to end.
func outputEventID(k Key, version int64) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%d", k, version))
	return "out-" + hex.EncodeToString(sum[:8])
}

// Code maps a Process error to a stable machine-readable code.
func Code(err error) string {
	var rl *upstream.RateLimitedError
	switch {
	case errors.As(err, &rl):
		return "RATE_LIMITED"
	case errors.Is(err, ErrInvalid):
		return "INVALID"
	case errors.Is(err, ErrUnknownProduct):
		return "UNKNOWN_PRODUCT"
	case errors.Is(err, ErrInactiveProduct):
		return "INACTIVE_PRODUCT"
	case errors.Is(err, ErrCountryDisabled):
		return "COUNTRY_NOT_ENABLED"
	case errors.Is(err, ErrBusy):
		return "LOCK_TIMEOUT"
	case errors.Is(err, ErrPublish):
		return "PUBLISH_FAILED"
	default:
		return "DEPENDENCY_FAILED"
	}
}
