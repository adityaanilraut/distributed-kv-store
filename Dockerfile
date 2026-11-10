FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /kv ./cmd/kv && \
    CGO_ENABLED=0 go build -trimpath -o /bench ./cmd/bench

FROM alpine:3.23
COPY --from=build /kv /bench /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/kv"]
