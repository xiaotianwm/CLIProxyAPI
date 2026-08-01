FROM golang:1.26-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential curl git && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=1 GOOS=linux go build -buildvcs=false -ldflags="-s -w -X 'main.Version=${VERSION}' -X 'main.Commit=${COMMIT}' -X 'main.BuildDate=${BUILD_DATE}'" -o ./CLIProxyAPI ./cmd/server/

ARG MANAGEMENT_PANEL_VERSION=v1.21.3-custom.8
ARG MANAGEMENT_PANEL_SHA256=4c5e9f32d70ceba18fcae3b4c3a4862ddf3d065309daf6bc209489813cd6c6fb

RUN curl -fsSL --retry 3 \
    "https://github.com/xiaotianwm/Cli-Proxy-API-Management-Center/releases/download/${MANAGEMENT_PANEL_VERSION}/management.html" \
    -o /tmp/management.html \
    && echo "${MANAGEMENT_PANEL_SHA256}  /tmp/management.html" | sha256sum -c -

FROM debian:bookworm

RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /CLIProxyAPI/static

COPY --from=builder ./app/CLIProxyAPI /CLIProxyAPI/CLIProxyAPI

COPY --from=builder /tmp/management.html /CLIProxyAPI/static/management.html

COPY config.example.yaml /CLIProxyAPI/config.example.yaml

WORKDIR /CLIProxyAPI

EXPOSE 8317

ENV TZ=Asia/Shanghai \
    MANAGEMENT_STATIC_PATH=/CLIProxyAPI/static

RUN cp /usr/share/zoneinfo/${TZ} /etc/localtime && echo "${TZ}" > /etc/timezone

CMD ["./CLIProxyAPI"]
