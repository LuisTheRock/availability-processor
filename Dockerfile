FROM golang:1.27.3-alpine AS builder

WORKDIR /app

COPY ./root/go.mod ./root/go.sum ./
RUN go mod download && go mod verify

COPY root/. .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags='-w -s' -o main .

FROM builder AS test

RUN go vet ./... && go test ./... && touch /tested

FROM gcr.io/distroless/static-debian13:nonroot AS runtime

WORKDIR /app

COPY --from=builder /app/main .

USER nonroot:nonroot

EXPOSE 5050

CMD ["./main"]
