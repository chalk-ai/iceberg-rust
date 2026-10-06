# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM golang:1.24.3-alpine@sha256:b4f875e650466fa0fe62c6fd3f02517a392123eea85f1d7e69d85f780e4db1c1 AS builder
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src

# Source checksums and linker metadata identify the same releases used by the fixtures.
FROM builder AS minio-build
ARG COMMIT=ecde75f9112f8410cb6cacb4b76193f1475b587e
RUN wget -O /tmp/source.tar.gz "https://codeload.github.com/minio/minio/tar.gz/${COMMIT}" \
    && echo "5ab9606123fb24b5d6f457b0d04025411339c17216647d9270ffc3eecea59b9e  /tmp/source.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/source.tar.gz --strip-components=1 \
    && rm /tmp/source.tar.gz
RUN go build -mod=readonly -tags kqueue -trimpath \
    -ldflags "-s -w -X github.com/minio/minio/cmd.Version=2025-05-24T17:08:30Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2025-05-24T17-08-30Z -X github.com/minio/minio/cmd.CommitID=${COMMIT} -X github.com/minio/minio/cmd.ShortCommitID=ecde75f9112f -X github.com/minio/minio/cmd.CopyrightYear=2025" \
    -o /out/minio .

FROM builder AS mc-build
ARG COMMIT=f71ad84bcf0fd4369691952af5d925347837dcec
RUN wget -O /tmp/source.tar.gz "https://codeload.github.com/minio/mc/tar.gz/${COMMIT}" \
    && echo "021460ac9adf491ea808e13ebb6c044d8846c175e88578798347a2ef4d361a5d  /tmp/source.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/source.tar.gz --strip-components=1 \
    && rm /tmp/source.tar.gz
RUN go build -mod=readonly -tags kqueue -trimpath \
    -ldflags "-s -w -X github.com/minio/mc/cmd.Version=2025-05-21T01:59:54Z -X github.com/minio/mc/cmd.ReleaseTag=RELEASE.2025-05-21T01-59-54Z -X github.com/minio/mc/cmd.CommitID=${COMMIT} -X github.com/minio/mc/cmd.ShortCommitID=f71ad84bcf0fd -X github.com/minio/mc/cmd.CopyrightYear=2025" \
    -o /out/mc .

FROM alpine:3.21.3@sha256:a8560b36e8b8210634f77d9f7f9efd7ffa463e380b75e2e74aff4511df3ef88c AS runtime
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/local/go/lib/time/zoneinfo.zip /usr/share/zoneinfo.zip
ENV ZONEINFO=/usr/share/zoneinfo.zip

FROM runtime AS minio
COPY --from=minio-build /out/minio /usr/bin/minio
COPY --from=minio-build /src/dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
COPY --from=minio-build /src/LICENSE /licenses/LICENSE
COPY --from=minio-build /src/CREDITS /licenses/CREDITS
ENV MINIO_ACCESS_KEY_FILE=access_key \
    MINIO_SECRET_KEY_FILE=secret_key \
    MINIO_ROOT_USER_FILE=access_key \
    MINIO_ROOT_PASSWORD_FILE=secret_key \
    MINIO_KMS_SECRET_KEY_FILE=kms_master_key \
    MINIO_CONFIG_ENV_FILE=config.env \
    MC_CONFIG_DIR=/tmp/.mc
EXPOSE 9000
VOLUME ["/data"]
ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["minio"]

FROM runtime AS mc
COPY --from=mc-build /out/mc /usr/bin/mc
COPY --from=mc-build /src/LICENSE /licenses/LICENSE
COPY --from=mc-build /src/CREDITS /licenses/CREDITS
ENTRYPOINT ["mc"]
