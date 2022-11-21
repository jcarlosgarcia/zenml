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

from .test_examples import run_example, validate_pipeline_run


def test_example() -> None:
    """Runs the kubeflow_pipelines_orchestration example.

    Args:
        tmp_path_factory: Factory to generate temporary test paths.
    """
    with run_example("kubeflow_pipelines_orchestration"):
        validate_pipeline_run(
            pipeline_name="mnist_pipeline",
            step_count=4,
            run_count=1,
        )
