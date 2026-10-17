# syntax=docker/dockerfile:1
# One image recipe for every component (D26):
#   docker build --build-arg CMD=migrate -t gpucp-migrate:dev .
# CMD is one of the directories under cmd/ (apiserver, controller, migrate).

ARG GO_VERSION=1.26

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build
ARG CMD
ARG TARGETOS
ARG TARGETARCH
RUN test -n "$CMD" || { echo "--build-arg CMD=<apiserver|controller|migrate> is required" >&2; exit 1; }
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN test -d "./cmd/$CMD" || { echo "unknown CMD: $CMD" >&2; exit 1; }
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/app "./cmd/$CMD"

# distroless static: no shell, no package manager, CA certs and tzdata only.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER 65532:65532
ENTRYPOINT ["/app"]
