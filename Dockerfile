# Dockerfile
FROM golang:1.25.6-alpine AS builder

WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o exchange-api ./api/exchange.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/exchange-api .
COPY --from=builder /app/etc /etc/exchange

EXPOSE 8888
CMD ["./exchange-api", "-f", "/etc/exchange/exchange-api.yaml"]