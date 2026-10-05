#!/usr/bin/env python3
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

"""Require named Rust tests to pass in the current Chalk consumer invocation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

CASE = re.compile(r"^test (\S+)(?: - should panic)? \.\.\. (ok|FAILED|ignored)\b", re.MULTILINE)
SUMMARY = re.compile(r"^test result: ok\. \d+ passed; 0 failed;", re.MULTILINE)


def verify(required: dict[str, list[str]], logs: Path) -> dict:
    packages: dict[str, dict] = {}
    errors: list[str] = []
    if not required:
        errors.append("No required packages")
    for package, names in required.items():
        result: dict = {"required": names, "cases": {}}
        packages[package] = result
        if not names or len(names) != len(set(names)):
            errors.append(f"{package}: required cases must be nonempty and unique")
        try:
            result["command"] = (logs / f"{package}.command").read_text().splitlines()
            result["exit_status"] = int((logs / f"{package}.status").read_text())
            text = (logs / f"{package}.log").read_text()
        except (OSError, ValueError) as exc:
            errors.append(f"{package}: cannot read invocation evidence: {exc}")
            continue
        if result["exit_status"] != 0:
            errors.append(f"{package}: test pipeline exited {result['exit_status']}")
        if len(SUMMARY.findall(text)) != 1:
            errors.append(f"{package}: expected one successful libtest summary")
        for name, outcome in CASE.findall(text):
            if name in result["cases"]:
                errors.append(f"{package}: duplicate outcome for {name}")
            result["cases"][name] = outcome
        for name in names:
            outcome = result["cases"].get(name, "missing")
            if outcome != "ok":
                errors.append(f"{package}: {name}: {outcome}; required ok")
    return {"packages": packages, "errors": errors}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs", type=Path, required=True)
    args = parser.parse_args()
    contract = Path(__file__).with_name("required-tests.json")
    report = verify(json.loads(contract.read_text()), args.logs)
    report["sha"] = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    report["build_url"] = os.environ.get("BUILDKITE_BUILD_URL")
    report["cargo_lock_sha256"] = hashlib.sha256(Path("Cargo.lock").read_bytes()).hexdigest()
    report["required_tests_sha256"] = hashlib.sha256(contract.read_bytes()).hexdigest()
    (args.logs / "evidence.json").write_text(json.dumps(report, indent=2) + "\n")
    for error in report["errors"]:
        print(error)
    print(f"Chalk consumer evidence: {len(report['packages'])} packages; {len(report['errors'])} errors")
    return int(bool(report["errors"]))


if __name__ == "__main__":
    raise SystemExit(main())
