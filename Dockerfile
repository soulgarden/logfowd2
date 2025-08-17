FROM rust:1.89-alpine AS builder

ENV RUSTFLAGS="-C target-feature=-crt-static"

RUN apk add --no-cache musl-dev pkgconfig openssl-dev

COPY . /tmp/rust/src/github.com/soulgarden/logfowd2

WORKDIR /tmp/rust/src/github.com/soulgarden/logfowd2

RUN cargo build --target=x86_64-unknown-linux-musl --release

FROM alpine:3.22

RUN apk add --no-cache libgcc

RUN adduser -S www-data -G www-data

COPY --from=builder --chown=www-data /tmp/rust/src/github.com/soulgarden/logfowd2/target/x86_64-unknown-linux-musl/release/logfowd2 /bin/logfowd2

RUN chmod +x /bin/logfowd2

USER www-data

CMD ["/bin/logfowd2"]
