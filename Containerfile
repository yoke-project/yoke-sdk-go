# The container this repository defines for its own build (prj_structure/95 §The verbs).
# It installs a runner; the floor that runner must meet is declared by the workspace (466).
FROM docker.io/library/golang:1.26-bookworm
ARG JUST_VERSION=1.58.0
RUN curl -fsSL https://just.systems/install.sh | bash -s -- --tag "${JUST_VERSION}" --to /usr/local/bin
WORKDIR /src
COPY . .
