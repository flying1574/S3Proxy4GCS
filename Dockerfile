# ---- Stage 1: Build ----
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /s3proxy main.go

# ---- Stage 2: Runtime ----
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /s3proxy /s3proxy

EXPOSE 8080

ENTRYPOINT ["/s3proxy"]
