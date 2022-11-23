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
import contextlib
import itertools
import logging
from typing import (
    TYPE_CHECKING,
    Generator,
    List,
    Tuple,
    Union,
)
from pydantic import Field

from tests.harness.model.base import BaseTestConfigModel
from tests.harness.model.deployment import DeploymentConfig
from tests.harness.model.requirements import TestRequirements


if TYPE_CHECKING:
    from tests.harness.deployment.base import BaseTestDeployment
    from tests.harness.harness import TestHarness
    from zenml.stack.stack_component import StackComponent
    from tests.harness.environment import TestEnvironment


class EnvironmentConfig(BaseTestConfigModel):
    """ZenML test environment settings."""

    name: str = Field(regex="^[a-z][a-z0-9-_]+$")
    description: str = ""
    deployment: Union[str, DeploymentConfig]
    requirements: List[Union[str, TestRequirements]] = Field(
        default_factory=list
    )

    def compile(self, harness: "TestHarness") -> None:
        """Validates and compiles the configuration when part of a test harness.

        Checks that the referenced deployment and requirements exist
        in the test harness configuration and replaces them with the
        actual configuration objects.

        Args:
            harness: The test harness to validate against.
        """
        if isinstance(self.deployment, str):
            deployment = harness.get_deployment_config(self.deployment)
            if deployment is None:
                raise ValueError(
                    f"Deployment '{self.deployment}' referenced by environment "
                    f"'{self.name}' does not exist."
                )
            self.deployment = deployment

        for i, config in enumerate(self.requirements):
            if isinstance(config, str):
                cfg = harness.get_global_requirements(config)
                if cfg is None:
                    raise ValueError(
                        f"Configuration '{config}' referenced by environment "
                        f"'{self.name}' does not exist."
                    )
                self.requirements[i] = cfg

    def get_environment(self) -> "TestEnvironment":
        """Instantiate a test environment based on this configuration.

        Returns:
            A test environment instance.
        """
        from tests.harness.environment import TestEnvironment
        from tests.harness.harness import TestHarness

        deployment = TestHarness().get_deployment(self.deployment.name)
        return TestEnvironment(config=self, deployment=deployment)
