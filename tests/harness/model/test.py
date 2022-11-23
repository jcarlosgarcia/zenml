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
import itertools
import logging
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Union,
)
from pydantic import Field

from tests.harness.model.base import BaseTestConfigModel
from tests.harness.model.requirements import TestRequirements


if TYPE_CHECKING:
    from tests.harness.harness import TestHarness
    from zenml.client import Client
    from zenml.stack import Stack
    from zenml.models.component_model import ComponentModel
    from zenml.stack.stack_component import StackComponent


class TestConfig(BaseTestConfigModel):
    """ZenML test module configuration."""

    module: str
    description: str = ""
    requirements: List[Union[str, TestRequirements]] = Field(
        default_factory=list
    )

    def compile(self, harness: "TestHarness") -> None:
        """Validates and compiles the configuration when part of a test harness.

        Checks that the referenced requirements exist in the test harness
        configuration and replaces them with the actual configuration objects.

        Args:
            harness: The test harness to validate against.
        """
        for i, config in enumerate(self.requirements):
            if isinstance(config, str):
                cfg = harness.get_global_requirements(config)
                if cfg is None:
                    raise ValueError(
                        f"Configuration '{config}' referenced by test "
                        f"configuration for module {self.module} does not "
                        f"exist."
                    )
                self.requirements[i] = cfg

    def check_requirements(
        self,
        client: "Client",
    ) -> Tuple[bool, Optional[str]]:
        """Check if all requirements are met.

        Args:
            client: The ZenML client to be used to check ZenML requirements.
            environment: An optional environment to use to enforce environment
                configurations.

        Returns:
            The true/false result and a message describing which requirements
            are not met.
        """
        for req in self.requirements:
            result, message = req.check_requirements(
                client, enforce_active_environment=True
            )
            if not result:
                return result, message

        return True, None

    @contextmanager
    def setup_test_stack(
        self,
        client: "Client",
        module: Optional[ModuleType] = None,
        cleanup: bool = True,
    ) -> Generator["Stack", None, None]:
        """Provision and activate a ZenML stack for this test.

        Args:
            module: A pytest test module.
            client: The ZenML client to be used to configure the ZenML stack.
            cleanup: Whether to clean up the stack after the test.

        Yields:
            The active stack that the test should use.
        """
        from zenml.utils.string_utils import random_str
        from zenml.models.stack_models import StackModel
        from zenml.enums import StackComponentType

        components: Dict[StackComponentType, "ComponentModel"] = {}

        stack_requirements = [req.stacks for req in self.requirements]
        requirements = itertools.chain.from_iterable(stack_requirements)

        for stack_requirement in requirements:
            component = stack_requirement.find_stack_component(
                client, enforce_active_environment=True
            )
            if component is None:
                # This should not happen if the requirements are checked
                # before calling this method.
                raise RuntimeError(
                    f"could not find a stack component that matches the test "
                    f"requirements '{stack_requirement}'."
                )
            if component.type in components:
                raise RuntimeError(
                    f"multiple stack components of type '{component.type}' "
                    f"are specified as requirements by the test requirements."
                )
            components[component.type] = [component.id]

        # Every stack needs an orchestrator
        if StackComponentType.ORCHESTRATOR not in components:
            orchestrator = client.zen_store._get_default_stack(
                project_name_or_id=client.active_project_name,
                user_name_or_id=client.active_user.id,
            ).components[StackComponentType.ORCHESTRATOR]
            components[StackComponentType.ORCHESTRATOR] = orchestrator

        # Every stack needs an artifact store
        if StackComponentType.ARTIFACT_STORE not in components:
            artifact_store = client.zen_store._get_default_stack(
                project_name_or_id=client.active_project_name,
                user_name_or_id=client.active_user.id,
            ).components[StackComponentType.ARTIFACT_STORE]
            components[StackComponentType.ARTIFACT_STORE] = artifact_store

        random_name = "pytest-"
        if module is not None:
            random_name = random_name + module.__name__.split(".")[-1]
        random_name = random_name + f"-{random_str(6).lower()}"

        stack = StackModel(
            name=random_name,
            user=client.active_user.id,
            project=client.active_project.id,
            components=components,
        )

        logging.info(f"Configuring and provisioning stack '{stack.name}'")

        # Register and activate the stack
        stack = client.register_stack(stack)
        current_active_stack = client.active_stack_model
        client.activate_stack(stack)

        # Provision the stack
        active_stack = client.active_stack
        try:
            active_stack.provision()
            active_stack.resume()
        except Exception:
            if cleanup:
                client.zen_store.delete_stack(stack.id)
            raise

        logging.info(f"Using active stack '{stack.name}'")

        # Yield the stack
        yield active_stack

        # Activate the previous stack
        client.activate_stack(current_active_stack)

        # Delete the stack
        if cleanup:
            client.zen_store.delete_stack(stack.id)
