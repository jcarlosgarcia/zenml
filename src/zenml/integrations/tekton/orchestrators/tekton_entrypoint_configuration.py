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
"""Implementation of the Tekton entrypoint configuration."""

import os
from typing import Any, List, Optional, Set

import kfp
from kubernetes import config as k8s_config
from tfx.orchestration.portable import data_types
from tfx.proto.orchestration.pipeline_pb2 import PipelineNode as Pb2PipelineNode

from zenml.entrypoints import StepEntrypointConfiguration
from zenml.integrations.tekton.orchestrators import utils
from zenml.steps import BaseStep

METADATA_UI_PATH_OPTION = "metadata_ui_path"


class TektonEntrypointConfiguration(StepEntrypointConfiguration):
    """Entrypoint configuration for running steps on tekton.

    This class writes a markdown file that will be displayed in the KFP UI.
    """

    @classmethod
    def get_custom_entrypoint_options(cls) -> Set[str]:
        """Tekton specific entrypoint options.

        The metadata ui path option expects a path where the markdown file
        that will be displayed in the tekton UI should be written. The same
        path needs to be added as an output artifact called
        `mlpipeline-ui-metadata` for the corresponding `kfp.dsl.ContainerOp`.

        Returns:
            The set of custom entrypoint options.
        """
        return {METADATA_UI_PATH_OPTION}

    @classmethod
    def get_custom_entrypoint_arguments(
        cls, step: BaseStep, *args: Any, **kwargs: Any
    ) -> List[str]:
        """Sets the metadata ui path argument to the value passed in via the keyword args.

        Args:
            step: The step that is being executed.
            *args: The positional arguments passed to the step.
            **kwargs: The keyword arguments passed to the step.

        Returns:
            A list of strings that will be used as arguments to the step.
        """
        return [
            f"--{METADATA_UI_PATH_OPTION}",
            kwargs[METADATA_UI_PATH_OPTION],
        ]

    def get_run_name(self, pipeline_name: str) -> str:
        """Returns the Tekton pipeline run name.

        Args:
            pipeline_name: The name of the pipeline.

        Returns:
            The Tekton pipeline run name.
        """
        k8s_config.load_incluster_config()
        run_id = os.environ["KFP_RUN_ID"]
        return kfp.Client().get_run(run_id).run.name  # type: ignore[no-any-return]

    def post_run(
        self,
        pipeline_name: str,
        step_name: str,
        pipeline_node: Pb2PipelineNode,
        execution_info: Optional[data_types.ExecutionInfo] = None,
    ) -> None:
        """Writes a markdown file that will display information.

        This will be about the step execution and input/output artifacts in the
        KFP UI.

        Args:
            pipeline_name: The name of the pipeline.
            step_name: The name of the step.
            pipeline_node: The pipeline node that is being executed.
            execution_info: The execution info of the step.
        """
        if execution_info:
            utils.dump_ui_metadata(
                node=pipeline_node,
                execution_info=execution_info,
                metadata_ui_path=self.entrypoint_args[METADATA_UI_PATH_OPTION],
            )