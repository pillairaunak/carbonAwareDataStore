# ---- Build Stage ----
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY carbonaware /app/carbonaware
COPY storage /app/storage
COPY util /app/util
COPY go.mod /app/
COPY visualizer /app/visualizer
COPY main.go /app/

RUN go build -o /app/minibtreestore_app .

# ---- Run Stage ----
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/minibtreestore_app /app/
COPY carbonaware /app/carbonaware
COPY storage /app/storage
COPY util /app/util
COPY go.mod /app/
COPY visualizer /app/visualizer
COPY main.go /app/
EXPOSE 8081
CMD ["./minibtreestore_app", "-carbonaware=true", "-mockhigh=true", "-inserts=500", "-buffersize=2", "-vizport=:8081", "-dir=./test_scenario1_data", "-interval=1s"]
