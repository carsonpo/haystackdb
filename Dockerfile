FROM --platform=linux/amd64 rust:slim-buster

# Create a new empty shell project
RUN USER=root cargo new --bin svc
WORKDIR /svc

# Copy over your manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# This build step will cache your dependencies
RUN cargo build --release
RUN rm src/*.rs

# Now copy your source code
COPY ./src ./src

# Build for release, reusing the cached dependencies
RUN cargo build --release

CMD ["./target/release/haystackdb"]