# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Image with gcsfuse installed and its package (.deb)
FROM gcr.io/deeplearning-platform-release/pytorch-gpu.1-12

# Allow non-root users to specify the allow_other or allow_root mount options
RUN echo "user_allow_other" > /etc/fuse.conf

RUN pip3 install timm

WORKDIR "/pytorch_dino/"

RUN git clone "https://github.com/facebookresearch/dino"

COPY perfmetrics/scripts/ml_tests/pytorch/run_model.sh ./

RUN mkdir -p "run_artifacts"
RUN mkdir -p "gcsfuse_data"

ARG PYTORCH_VERSION
ARG BUCKET_TYPE
ENV PYTORCH_VERSION=${PYTORCH_VERSION}
ENV BUCKET_TYPE=${BUCKET_TYPE}

ENTRYPOINT ["/bin/bash", "-c", "./run_model.sh ${PYTORCH_VERSION} ${BUCKET_TYPE}"]
