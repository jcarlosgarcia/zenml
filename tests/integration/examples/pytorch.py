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

from .utils import run_example, validate_pipeline_run


def test_example() -> None:
    """Runs the pytorch example."""
    with run_example("pytorch"):
        validate_pipeline_run(
            pipeline_name="fashion_mnist_pipeline",
            step_count=3,
            run_count=1,
        )
