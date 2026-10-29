# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN uv pip install --system --no-cache "pyspark[connect]==4.0.1"

COPY provision.py /opt/spark/provision.py

ENTRYPOINT ["/bin/sh", "-c", "python /opt/spark/provision.py && touch /tmp/provision_complete && tail -f /dev/null"]
