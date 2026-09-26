FROM rust:1.98.1-alpine3.24 AS builder

RUN apk add --no-cache pkgconfig make

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo build --locked --release

FROM alpine:3.24

RUN adduser -S www-data -G www-data

COPY --from=builder --chmod=0555 /app/target/release/logfowd2 /bin/logfowd2

USER www-data

CMD ["/bin/logfowd2"]
