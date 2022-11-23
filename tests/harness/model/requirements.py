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
from enum import Enum
import itertools
import logging
import os
import platform
import shutil
from types import ModuleType
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple
import pkg_resources
from pydantic import BaseModel, Extra, Field, SecretStr

from tests.harness.model.base import BaseTestConfigModel
from zenml.enums import StackComponentType

if TYPE_CHECKING:
    from tests.harness.model.environment import EnvironmentConfig
    from zenml.client import Client
    from zenml.stack import Stack
    from zenml.models.component_model import ComponentModel
    from zenml.stack.stack_component import StackComponent


class OSType(str, Enum):
    """Enum for the different types of operating systems."""

    LINUX = "linux"
    MACOS = "macos"
    WINDOWS = "windows"


class StackRequirement(BaseTestConfigModel):
    """ZenML stack component descriptor."""

    name: str = ""
    type: StackComponentType
    flavor: Optional[str] = None
    configuration: Optional[Dict[str, Any]] = None

    def find_stack_component(
        self,
        client: "Client",
        enforce_active_environment: bool = False,
    ) -> "ComponentModel":
        """Find a stack component that meets these requirements.

        The current deployment (accessible through the passed client) is
        searched for all components that match the requirements criteria. If
        supplied, the environment is used to filter the components further
        based on the stack configurations mandated by the environment.

        Args:
            client: The ZenML client to be used.
            enforce_active_environment: Whether to enforce the requirements
                mandated by the active environment.

        Returns:
            The stack component or None if no component was found.
        """
        from tests.harness.harness import TestHarness

        components = client.list_stack_components_by_type(self.type)

        # TODO: we should be able to use a client method to filter stack
        # components by flavor instead of having to do this here.
        def filter_components(component: "ComponentModel") -> bool:
            if self.name:
                return component.name == self.name
            if self.flavor and component.flavor != self.flavor:
                return False
            if self.configuration:
                for key, value in self.configuration.items():
                    if component.configuration.get(key) != value:
                        return False

            return True

        # Filter components further by flavor and configuration.
        components = list(filter(filter_components, components))

        if enforce_active_environment:
            environment = TestHarness().active_environment
            # Filter components further by the restrictions mandated by the
            # active environment.
            # TODO: components = environment.filter_stack_components(components)

        if len(components) == 0:
            return None

        return components[0]

    def check_requirements(
        self,
        client: "Client",
        enforce_active_environment: bool = False,
    ) -> Tuple[bool, Optional[str]]:
        """Check if the requirements are met.

        Args:
            client: The ZenML client to be used to check ZenML requirements.
            enforce_active_environment: Whether to enforce the requirements
                mandated by the active environment.

        Returns:
            The true/false result and a message describing which requirements
            are not met.
        """
        component = self.find_stack_component(
            client, enforce_active_environment=enforce_active_environment
        )
        if component is not None:
            return True, None

        msg = f"missing {self.type.value}"
        if self.flavor is not None:
            msg += f" with flavor '{self.flavor}'"
        if self.configuration:
            config_msg = ", ".join(
                f"{k}={v}" for k, v in self.configuration.items()
            )
            msg += f" with configuration '{config_msg}'"
        return (False, msg)

    def get_integration(self, client: "Client") -> Optional[str]:
        """Get the integration requirement implied by this stack requirement.

        Args:
            client: The ZenML client to be used.

        Returns:
            The integration name, if one is implied by this stack requirement.
        """
        if self.flavor is None:
            return None

        flavor = client.get_flavor_by_name_and_type(
            name=self.flavor, component_type=self.type
        )

        return flavor.integration

    def register_component(self, client: "Client") -> "ComponentModel":
        """Register a stack component described by these requirements.

        Args:
            client: The ZenML client to be used to provision components.
        """
        from zenml.models.component_model import ComponentModel
        from zenml.utils.string_utils import random_str

        if self.flavor is None:
            raise ValueError(
                f"cannot register component of type '{self.type.value}' without "
                "specifying a flavor"
            )

        return client.register_stack_component(
            ComponentModel(
                name=self.name or f"pytest-{random_str(6).lower()}",
                user=client.active_user.id,
                project=client.active_project.id,
                type=self.type,
                flavor=self.flavor,
                configuration=self.configuration or {},
            )
        )


class TestRequirements(BaseTestConfigModel):
    """Test requirements descriptor."""

    name: str = ""
    integrations: List[str] = Field(default_factory=list)
    packages: List[str] = Field(default_factory=list)
    system_tools: List[str] = Field(default_factory=list)
    system_os: List[OSType] = Field(default_factory=list)
    stacks: List[StackRequirement] = Field(default_factory=list)

    def check_software_requirements(self) -> Tuple[bool, Optional[str]]:
        """Check if the software requirements are met.

        Returns:
            The true/false result and a message describing which requirements
            are not met.
        """
        from zenml.integrations.registry import integration_registry

        if self.system_os:
            this_os = platform.system().lower().replace("darwin", "macos")
            if this_os not in self.system_os:
                return False, f"unsupported operating system '{this_os}'"

        missing_system_tools = []
        for tool in self.system_tools:
            if not shutil.which(tool):
                missing_system_tools.append(tool)
        if missing_system_tools:
            return (
                False,
                f"missing system tools: {', '.join(missing_system_tools)}",
            )

        missing_integrations = []

        integrations = self.integrations.copy()

        for integration in integrations:
            if not integration_registry.is_installed(integration):
                missing_integrations.append(integration)

        if missing_integrations:
            return (
                False,
                f"missing integrations: {', '.join(missing_integrations)}",
            )

        try:
            for p in self.packages:
                pkg_resources.get_distribution(p)
        except pkg_resources.DistributionNotFound as e:
            return False, f"missing package: {e}"

        return True, None

    def check_stack_requirements(
        self,
        client: "Client",
        enforce_active_environment: bool = False,
    ) -> Tuple[bool, Optional[str]]:
        """Check if the requirements are met.

        Args:
            client: The ZenML client to be used to check ZenML requirements.
            enforce_active_environment: Whether to enforce the requirements
                mandated by the active environment.

        Returns:
            The true/false result and a message describing which requirements
            are not met.
        """
        from zenml.integrations.registry import integration_registry

        missing_integrations = []
        integrations = []
        for stack in self.stacks:
            integration = stack.get_integration(client)
            if integration is not None:
                integrations.append(integration)

        for integration in integrations:
            if not integration_registry.is_installed(integration):
                missing_integrations.append(integration)

        if missing_integrations:
            return (
                False,
                f"missing integrations: {', '.join(missing_integrations)}",
            )

        for stack_requirement in self.stacks:
            result, message = stack_requirement.check_requirements(
                client=client,
                enforce_active_environment=enforce_active_environment,
            )
            if not result:
                return result, message

        return True, None

    def check_requirements(
        self,
        client: "Client",
        enforce_active_environment: bool = False,
    ) -> Tuple[bool, Optional[str]]:
        """Check if all requirements are met.

        Args:
            client: The ZenML client to be used to check ZenML requirements.
            enforce_active_environment: Whether to enforce the requirements
                mandated by the active environment

        Returns:
            The true/false result and a message describing which requirements
            are not met.
        """
        result, message = self.check_software_requirements()
        if not result:
            return result, message

        return self.check_stack_requirements(
            client=client, enforce_active_environment=enforce_active_environment
        )
