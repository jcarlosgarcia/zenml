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

from typing import Optional
from tests.harness.deployment.base import BaseTestDeployment

from tests.harness.model import (
    DeploymentConfig,
    DeploymentSetup,
    DeploymentType,
    DeploymentStoreConfig,
)


class ServerDockerComposeTestDeployment(BaseTestDeployment):
    """A deployment that runs a ZenML server as a docker-compose service."""

    def __init__(self, config: DeploymentConfig) -> None:
        super().__init__(config)

    @property
    def is_running(self) -> bool:

        return False

    def up(self) -> None:
        raise NotImplementedError(
            "The docker-compose deployment is not yet implemented."
        )

    def down(self) -> None:
        raise NotImplementedError(
            "The docker-compose deployment is not yet implemented."
        )

    def get_store_config(self) -> Optional[DeploymentStoreConfig]:
        raise NotImplementedError(
            "The docker-compose deployment is not yet implemented."
        )


ServerDockerComposeTestDeployment.register_deployment_class(
    type=DeploymentType.SERVER, setup=DeploymentSetup.DOCKER_COMPOSE
)
