# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder

RUN apk add --no-cache git tzdata

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /out/scheduler cmd/scheduler/main.go && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /out/worker cmd/worker/main.go && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /out/jobctl cmd/jobctl/main.go


FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="Job Scheduler" \
      org.opencontainers.image.description="Distributed Job Scheduler (Raft + gRPC)" \
      org.opencontainers.image.source="https://github.com/${GITHUB_REPOSITORY}"

WORKDIR /

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder --chown=nonroot:nonroot /out/scheduler /scheduler
COPY --from=builder --chown=nonroot:nonroot /out/worker /worker
COPY --from=builder --chown=nonroot:nonroot /out/jobctl /jobctl

USER nonroot:nonroot

CMD ["/scheduler"]
