FROM rust:slim-bookworm AS build

WORKDIR /anyseek

# Install C/C++ compiler
RUN apt-get update && apt-get install -y g++

COPY . /anyseek

RUN cargo build --release --features server --bin anyseek_server

FROM debian:bookworm-slim

LABEL org.opencontainers.image.title="anyseek server"
LABEL org.opencontainers.image.source="https://github.com/TheLudlows/seekpower"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /anyseek/target/release/anyseek_server /
COPY LICENSE /LICENSE
USER root

ENTRYPOINT ["./anyseek_server","local_ip=0.0.0.0","local_port=80","index_path=anyseek_index"]
