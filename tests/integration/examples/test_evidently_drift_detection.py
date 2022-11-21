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

from zenml.enums import ExecutionStatus
from zenml.post_execution.pipeline import get_pipeline
from .utils import run_example


def test_example() -> None:
    """Runs the evidently_drift_detection example."""

    from evidently.model_profile import Profile  # type: ignore[import]

    with run_example("evidently_drift_detection"):

        pipeline = get_pipeline("drift_detection_pipeline")
        assert pipeline

        run = pipeline.runs[-1]
        assert run.status == ExecutionStatus.COMPLETED
        assert len(run.steps) == 3

        # Final step should have output a data drift report
        drift_detection_step = run.get_step("drift_detector")
        output = drift_detection_step.outputs["profile"].read()
        assert isinstance(output, Profile)
