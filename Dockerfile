FROM golang:1.24 as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/api ./cmd/api && CGO_ENABLED=0 GOOS=linux go build -o bin/remote ./cmd/remote

FROM scratch as api

WORKDIR /app

COPY --from=builder /app/bin/api .

EXPOSE 5000
EXPOSE 4001

CMD ["./api"]

FROM scratch as remote

WORKDIR /app

COPY --from=builder /app/bin/remote .

EXPOSE 4000
CMD ["./remote"]