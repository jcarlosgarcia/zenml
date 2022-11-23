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
from contextlib import contextmanager
import os
import shutil
from pathlib import Path
from typing import Generator

from tests.conftest import cleanup_folder

from zenml.cli import EXAMPLES_RUN_SCRIPT, SHELL_EXECUTABLE, LocalExample
from zenml.enums import ExecutionStatus
from zenml.post_execution.pipeline import get_pipeline


def copy_example_files(example_dir: str, dst_dir: str) -> None:
    for item in os.listdir(example_dir):
        if item == ".zen":
            # don't copy any existing ZenML repository
            continue

        s = os.path.join(example_dir, item)
        d = os.path.join(dst_dir, item)
        if os.path.isdir(s):
            shutil.copytree(s, d)
        else:
            shutil.copy2(s, d)


def example_runner(examples_dir):
    """Get the executable that runs examples.

    By default, returns the path to an executable .sh file in the
    repository, but can also prefix that with the path to a shell
    / interpreter when the file is not executable on its own. The
    latter option is needed for Windows compatibility.
    """
    return (
        [os.environ[SHELL_EXECUTABLE]] if SHELL_EXECUTABLE in os.environ else []
    ) + [str(examples_dir / EXAMPLES_RUN_SCRIPT)]


@contextmanager
def run_example(
    name: str,
    *args: str,
) -> Generator[LocalExample, None, None]:
    """Runs the given examples and validates they ran correctly.

    Args:
        name: The name (=directory name) of the example
        *args: Additional arguments to pass to the example
    """

    # Root directory of all checked out examples
    examples_directory = Path(__file__).parents[3] / "examples"

    dst_dir = Path(os.getcwd()) / name
    dst_dir.mkdir()

    # Copy all example files into the repository directory
    copy_example_files(str(examples_directory / name), str(dst_dir))

    # Run the example
    example = LocalExample(name=name, path=dst_dir)
    example.run_example_directly(*args)

    yield example

    # Clean up
    cleanup_folder(dst_dir)


def validate_pipeline_run(
    pipeline_name: str, step_count: int, run_count: int = 1
):
    """A basic example validation function.

    This function will make sure the runs of a specific pipeline succeeded by
    checking the run status as well as making sure all steps were executed.

    Args:
        pipeline_name: The name of the pipeline to verify.
        step_count: The amount of steps inside the pipeline.
        run_count: The amount of pipeline runs to verify.

    Raises:
        AssertionError: If the validation failed.
    """

    pipeline = get_pipeline(pipeline_name)
    assert pipeline

    for run in pipeline.runs[-run_count:]:
        assert run.status == ExecutionStatus.COMPLETED
        assert len(run.steps) == step_count
