# Keep this version in step with rust-toolchain.toml.
FROM rust:1.98-slim-bookworm AS build

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# sqlx::migrate!() reads this directory at *compile* time and embeds the SQL in the binary, so
# the migrations have to be present for the build — and need not be present at runtime.
COPY migrations ./migrations
# Same story: `handlers::docs` embeds this with include_str!, so `GET /openapi.yaml` is served
# out of the binary. Needed for the build, and absent from the distroless image below.
COPY openapi.yaml ./
RUN cargo build --release --locked

# Distroless: glibc and nothing else — no shell, no package manager, no utilities for an
# attacker to reach for. `cc-debian12` matches the bookworm glibc the binary was linked
# against, and the `nonroot` tag runs as uid 65532 without needing a root step to create it.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /build/target/release/business /usr/local/bin/business

# Bind every interface in the container, so the port published by `docker run -p` is
# reachable. The binary itself defaults to 127.0.0.1 for local runs.
ENV HOST=0.0.0.0 \
    PORT=3001
EXPOSE 3001

# Docker has no idea what the app serves, so the check has to be spelled out. The binary
# probes itself over loopback, so the image needs neither an HTTP client nor a shell.
# `start-period` is longer than the mock's: this process connects to Postgres and runs
# migrations before it ever binds the port.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["business", "--health"]

CMD ["business"]
