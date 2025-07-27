FROM golang:1.24 as builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/api ./cmd/api && CGO_ENABLED=0 GOOS=linux go build -o bin/database ./cmd/database

FROM scratch as api

WORKDIR /app

COPY --from=builder /app/bin/api .

EXPOSE 5000
EXPOSE 4001

CMD ["./api"]

FROM scratch as database

WORKDIR /app

COPY --from=builder /app/bin/database .

EXPOSE 4000
CMD ["./database"]