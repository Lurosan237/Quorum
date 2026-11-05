# The server is one static binary; the image is that binary and nothing else (no base image to pull).
# Build it with:  CGO_ENABLED=0 go build -o bin/quorum-server ./cmd/quorum-server && docker build -t quorum .
FROM scratch
COPY bin/quorum-server /quorum-server
EXPOSE 7000
VOLUME /data
ENTRYPOINT ["/quorum-server", "-listen", ":7000", "-data", "/data"]
