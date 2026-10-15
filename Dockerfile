FROM rust:slim-bookworm AS build

WORKDIR /seekpower

# Install C/C++ compiler
RUN apt-get update && apt-get install -y g++

COPY . /seekpower

RUN cargo build --release

FROM debian:bookworm-slim

LABEL org.opencontainers.image.title="Seekpower server"
LABEL org.opencontainers.image.source="https://github.com/TheLudlows/seekpower"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /seekpower/target/release/seekpower_server /
COPY LICENSE /LICENSE
USER root

ENTRYPOINT ["./seekpower_server","local_ip=0.0.0.0","local_port=80","index_path=seekstorm_index"]
