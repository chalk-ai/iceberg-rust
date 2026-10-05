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

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("checker", Path(__file__).with_name("check-required-tests.py"))
assert spec is not None and spec.loader is not None
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.logs = Path(temporary.name)
        self.contract = {"iceberg": ["tests::required"]}
        (self.logs / "iceberg.command").write_text("cargo\ntest\n--features\nstorage-gcs\n")
        (self.logs / "iceberg.status").write_text("0\n")
        self.write_log("test tests::required ... ok\n")

    def write_log(self, cases: str):
        (self.logs / "iceberg.log").write_text(cases + "test result: ok. 1 passed; 0 failed; 0 ignored\n")

    def errors(self):
        return checker.verify(self.contract, self.logs)["errors"]

    def test_success_and_should_panic_results(self):
        for suffix in ("", " - should panic"):
            self.write_log(f"test tests::required{suffix} ... ok\n")
            self.assertEqual(self.errors(), [])

    def test_missing_ignored_failed_and_listed_cases_cannot_pass(self):
        for case in (
            "",
            "tests::required: test\n",
            "test tests::required ... ignored\n",
            "test tests::required ... FAILED\n",
        ):
            self.write_log(case)
            self.assertTrue(self.errors(), case)

    def test_retry_cannot_hide_an_unsuccessful_attempt(self):
        for outcome in ("ignored", "FAILED"):
            for first, second in ((outcome, "ok"), ("ok", outcome)):
                self.write_log(f"test tests::required ... {first}\ntest tests::required ... {second}\n")
                self.assertTrue(self.errors())

    def test_post_test_crash_cannot_pass(self):
        (self.logs / "iceberg.status").write_text("139\n")
        self.assertIn("test pipeline exited 139", " ".join(self.errors()))

    def test_missing_or_corrupt_status_and_logs_fail(self):
        for status in ("", "not a status"):
            (self.logs / "iceberg.status").write_text(status)
            self.assertTrue(self.errors())
        (self.logs / "iceberg.status").write_text("0\n")
        (self.logs / "iceberg.log").unlink()
        self.assertTrue(self.errors())

    def test_incomplete_run_cannot_pass(self):
        (self.logs / "iceberg.log").write_text("test tests::required ... ok\n")
        self.assertTrue(self.errors())

    def test_contract_must_be_nonempty_and_unique(self):
        for contract in ({}, {"iceberg": []}, {"iceberg": ["tests::required", "tests::required"]}):
            self.assertTrue(checker.verify(contract, self.logs)["errors"])

    def test_case_in_one_package_cannot_cover_another(self):
        self.contract["iceberg-catalog-glue"] = ["tests::required"]
        self.assertIn("iceberg-catalog-glue: cannot read", " ".join(self.errors()))


if __name__ == "__main__":
    unittest.main()
