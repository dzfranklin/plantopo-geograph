FROM golang:1.23-bookworm

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN GOOS=linux go build ./cmd/api

ENTRYPOINT ["/app/api"]
