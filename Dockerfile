FROM golang:1.26.11-alpine AS build
ARG GOPROXY
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,id=overmindv-gomod,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,id=overmindv-gomod,target=/go/pkg/mod \
    --mount=type=cache,id=overmindv-gobuild,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/recommendations ./cmd/recommendations

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata wget && addgroup -S recommendations && adduser -S recommendations -G recommendations
COPY --from=build /out/recommendations /usr/local/bin/recommendations
COPY --from=build /src/migrations /app/migrations
WORKDIR /app
USER recommendations
EXPOSE 8080
ENTRYPOINT ["recommendations"]
