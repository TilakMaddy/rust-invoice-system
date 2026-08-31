# Keep this version in step with rust-toolchain.toml.
FROM rust:1.98-slim-bookworm AS build

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

# Distroless: glibc and nothing else — no shell, no package manager, no utilities for an
# attacker to reach for. `cc-debian12` matches the bookworm glibc the binary was linked
# against, and the `nonroot` tag runs as uid 65532 without needing a root step to create it.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /build/target/release/mock-payment-service /usr/local/bin/mock-payment-service

# Bind every interface in the container, so the port published by `docker run -p` is
# reachable. The binary itself defaults to 127.0.0.1 for local runs.
ENV HOST=0.0.0.0 \
    PORT=3000
EXPOSE 3000

# Docker has no idea what the app serves, so the check has to be spelled out. The binary
# probes itself over loopback, so the image needs neither an HTTP client nor a shell.
HEALTHCHECK --interval=10s --timeout=3s --start-period=2s --retries=3 \
    CMD ["mock-payment-service", "--health"]

CMD ["mock-payment-service"]
