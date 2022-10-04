FROM rust:1.63-alpine3.16 as builder

RUN apk add --no-cache musl-dev

COPY . /tmp/rust/src/github.com/soulgarden/logfowd2

WORKDIR /tmp/rust/src/github.com/soulgarden/logfowd2

RUN cargo build --release

FROM alpine:3.16

RUN adduser -S www-data -G www-data

COPY --from=builder --chown=www-data /tmp/rust/src/github.com/soulgarden/logfowd2/target/release/logfowd2 /bin/logfowd2

RUN chmod +x /bin/logfowd2

USER www-data

CMD ["/bin/logfowd2"]
