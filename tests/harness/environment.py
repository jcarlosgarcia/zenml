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

from abc import ABCMeta
from contextlib import contextmanager
import logging
import os
from pathlib import Path
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    cast,
)
from pydantic import BaseModel, Extra, Field, ValidationError, validator
from pydantic.main import ModelMetaclass

import yaml
from tests.harness.deployment.base import BaseTestDeployment
from tests.harness.model import (
    Configuration,
    DeploymentConfig,
    EnvironmentConfig,
    Secret,
    TestConfig,
    TestRequirements,
)
from tests.harness.model.requirements import StackRequirement

if TYPE_CHECKING:
    from zenml.client import Client
    from zenml.stack import Stack


class TestEnvironment:
    """ZenML test environment."""

    def __init__(
        self,
        config: EnvironmentConfig,
        deployment: BaseTestDeployment,
    ) -> None:
        """Initialize the test environment.

        Args:
            config: The environment configuration.
            deployment: The deployment configured for this environment.
        """
        self.config = config
        self.deployment = deployment

    def check_software_requirements(self) -> None:
        """Check if the software requirements for this environment are met."""
        for requirement in self.config.requirements:
            result, err = requirement.check_software_requirements()

            if not result:
                raise RuntimeError(
                    f"Software requirement '{requirement.name}' not met: {err} "
                    f"for environment '{self.config.name}'. Please install "
                    f"the required software packages and tools and try again."
                )

    @property
    def is_running(self) -> bool:
        """Returns whether the environment is running."""
        return self.deployment.is_running

    @property
    def is_provisioned(self) -> bool:
        """Returns whether the environment is provisioned."""
        from zenml.models.component_model import ComponentModel

        if not self.deployment.is_running:
            return False

        with self.deployment.connect() as client:

            component_requirements: List[StackRequirement] = []

            for requirement in self.config.requirements:
                component_requirements.extend(requirement.stacks)

            for component_requirement in component_requirements:
                component_model = component_requirement.find_stack_component(
                    client=client,
                )
                if component_model is None:
                    return False

        return True

    def up(self) -> None:
        """Start the deployment for this environment."""
        self.check_software_requirements()
        self.deployment.up()

    def provision(self) -> None:
        """Start the deployment for this environment and provision the stack components."""
        from zenml.stack.stack_component import StackComponent
        from zenml.models.component_model import ComponentModel

        self.check_software_requirements()
        self.deployment.up()

        with self.deployment.connect() as client:

            component_requirements: List[StackRequirement] = []
            components: List[ComponentModel] = []

            for requirement in self.config.requirements:
                component_requirements.extend(requirement.stacks)

            for component_requirement in component_requirements:
                component_model = component_requirement.find_stack_component(
                    client=client,
                )
                if component_model is not None:
                    logging.info(
                        f"Reusing existing stack component "
                        f"'{component_model.name}'"
                    )
                else:
                    component_model = component_requirement.register_component(
                        client=client,
                    )
                    logging.info(
                        f"Registered stack component '{component_model.name}'"
                    )
                components.append(component_model)

            for component_model in components:

                component = StackComponent.from_model(
                    component_model=component_model
                )

                if not component.is_running:
                    logging.info(f"Provisioning component '{component.name}'")
                    if not component.is_provisioned:
                        try:
                            component.provision()
                        except NotImplementedError:
                            pass
                    if not component.is_running:
                        try:
                            component.resume()
                        except NotImplementedError:
                            pass

    def deprovision(self) -> None:
        """Deprovision all stack components for this environment."""
        from zenml.stack.stack_component import StackComponent
        from zenml.models.component_model import ComponentModel

        if not self.is_running:
            logging.info(
                f"Environment '{self.config.name}' is not running, "
                f"skipping deprovisioning."
            )
            return

        with self.deployment.connect() as client:

            component_requirements: List[StackRequirement] = []
            components: List[ComponentModel] = []

            for requirement in self.config.requirements:
                component_requirements.extend(requirement.stacks)

            for component_requirement in component_requirements:
                component_model = component_requirement.find_stack_component(
                    client=client,
                )
                if component_model is None:
                    logging.info(
                        f"Stack component {component_requirement.name} "
                        f"no longer registered."
                    )
                else:
                    components.append(component_model)

            for component_model in components:
                component = StackComponent.from_model(
                    component_model=component_model
                )
                logging.info(f"Deprovisioning component '{component.name}'")

                if not component.is_suspended:
                    try:
                        component.suspend()
                    except NotImplementedError:
                        pass
                if component.is_provisioned:
                    try:
                        component.deprovision()
                    except NotImplementedError:
                        pass

            for component_model in components:
                logging.info(f"Deleting component '{component_model.name}'")
                client.zen_store.delete_stack_component(component_model.id)

    def down(self) -> None:
        """Deprovision stacks and stop the deployment for this environment."""
        self.deprovision()
        self.deployment.down()

    def cleanup(self) -> None:
        """Clean up the deployment for this environment."""
        self.deprovision()
        self.deployment.cleanup()

    @contextmanager
    def setup(
        self,
        tear_down: bool = True,
        cleanup_stacks: bool = True,
    ) -> Generator["Client", None, None]:
        """Context manager to provision the environment and optionally tear it down afterwards.

        Args:
            tear_down: Whether to deprovision the stacks and tear down the
                environment on exit.
            cleanup_stacks: Whether to deprovision the stacks on exit.

        Yields:
            A ZenML client connected to the environment.
        """
        try:
            self.provision()
        except Exception as e:
            logging.error(
                f"Failed to provision environment '{self.config.name}': {e}"
            )
            try:
                if tear_down:
                    self.cleanup()
                elif cleanup_stacks:
                    self.deprovision()
            except Exception as e:
                logging.error(
                    f"Failed to cleanup environment '{self.config.name}': {e}"
                )
                raise e

        with self.deployment.connect() as client:
            yield client

        try:
            if tear_down:
                self.cleanup()
            elif cleanup_stacks:
                self.deprovision()
        except Exception as e:
            logging.error(
                f"Failed to cleanup environment '{self.config.name}': {e}"
            )
            raise e
