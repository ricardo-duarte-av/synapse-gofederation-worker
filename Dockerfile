FROM golang:1.25-alpine AS build
WORKDIR /src

# Download dependencies first so they cache independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# .dockerignore strips .git, so build info is passed in rather than derived.
ARG TAG=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

# CGO_ENABLED=0 for the distroless static base below: every dependency here is
# pure Go, pgx included, so there is nothing to link against.
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w \
        -X main.tag=${TAG} \
        -X main.commit=${COMMIT} \
        -X main.buildTime=${BUILD_TIME}" \
      -o /out/gofederation-worker ./cmd/gofederation-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gofederation-worker /gofederation-worker
ENTRYPOINT ["/gofederation-worker"]
CMD ["-config", "/data/gofederation-worker.yaml"]
