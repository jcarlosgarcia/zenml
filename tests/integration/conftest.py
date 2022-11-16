#  Copyright (c) ZenML GmbH 2022. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_on_windows: mark tests that should be skipped on Windows",
    )


def pytest_collection_modifyitems(config, items):
    # skip tests that are not supported on Windows
    skip_scope = pytest.mark.skip(reason="Test not supported on Windows")
    for item in items:
        if "skip_on_windows" in item.keywords:
            item.add_marker(skip_scope)
