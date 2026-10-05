// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::Debug;

use anyhow::anyhow;
use iceberg::{Error, ErrorKind};

use crate::utils::AWS_GLUE_SDK_RETRY_MAX_ATTEMPTS;

/// Format AWS SDK error into iceberg error
pub(crate) fn from_aws_sdk_error<T>(error: aws_sdk_glue::error::SdkError<T>) -> Error
where T: Debug {
    Error::new(
        ErrorKind::Unexpected,
        format!(
            "Operation failed after applying AWS Glue SDK retry policy (max_attempts={AWS_GLUE_SDK_RETRY_MAX_ATTEMPTS})"
        ),
    )
    .with_source(anyhow!("aws sdk error: {error:?}"))
}

/// Format AWS Build error into iceberg error
pub(crate) fn from_aws_build_error(error: aws_sdk_glue::error::BuildError) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Operation failed for hitting aws build error".to_string(),
    )
    .with_source(anyhow!("aws build error: {error:?}"))
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use aws_sdk_glue::error::SdkError;

    use super::*;

    #[test]
    fn test_aws_sdk_error_mentions_retry_policy() {
        let sdk_error: SdkError<()> = SdkError::timeout_error(anyhow!("timed out"));

        let error = from_aws_sdk_error(sdk_error);

        assert!(error.message().contains(&format!(
            "after applying AWS Glue SDK retry policy (max_attempts={AWS_GLUE_SDK_RETRY_MAX_ATTEMPTS})"
        )));
    }
}
