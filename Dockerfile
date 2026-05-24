FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o hermes .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/hermes .
EXPOSE 8000
CMD ["./hermes"]