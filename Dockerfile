FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG APP_NAME
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/app ./cmd/${APP_NAME}/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /bin/app /app/server

CMD ["/app/server"]