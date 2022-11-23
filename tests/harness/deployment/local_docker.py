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

import logging
import time
from docker.models.containers import Container
import docker.errors as docker_errors

from typing import Optional
from tests.harness.deployment.base import BaseTestDeployment

from tests.harness.model import (
    DeploymentConfig,
    DeploymentSetup,
    DeploymentType,
    DeploymentStoreConfig,
)


MYSQL_DOCKER_IMAGE = "mysql:5.7"
MYSQL_DEFAULT_PASSWORD = "zenml"
MYSQL_DEFAULT_PORT = 3306
MYSQL_DOCKER_CONTAINER_NAME_PREFIX = "zenml-mysql-"


class LocalDockerTestDeployment(BaseTestDeployment):
    """A deployment that uses a MySQL Docker container to host the ZenML database."""

    def __init__(self, config: DeploymentConfig) -> None:
        super().__init__(config)

    @property
    def container_name(self) -> str:
        return f"{MYSQL_DOCKER_CONTAINER_NAME_PREFIX}-{self.config.name}"

    @property
    def container(self) -> Optional[Container]:
        """Returns the Docker container configured for the deployment.

        Returns:
            The container for the deployment if it exists, None otherwise.
        """
        try:
            return self.docker_client.containers.get(self.container_name)
        except docker_errors.NotFound:
            return None

    @property
    def is_running(self) -> bool:

        # Check if container already exists
        container = self.container
        if container and container.status == "running":
            return True

        return False

    def up(self) -> None:
        from zenml.utils.networking_utils import scan_for_available_port

        if self.is_running:

            logging.info(
                f"Deployment '{self.config.name}' is already running. "
                f"Skipping provisioning."
            )
            return

        # Cleanup a previous deployment in a failed state
        self.down()

        port = scan_for_available_port(MYSQL_DEFAULT_PORT)

        if port is None:
            raise RuntimeError("Could not find an available port for MySQL.")
        self.docker_client.containers.run(
            name=self.container_name,
            image=MYSQL_DOCKER_IMAGE,
            detach=True,
            environment={"MYSQL_ROOT_PASSWORD": MYSQL_DEFAULT_PASSWORD},
            remove=True,
            auto_remove=True,
            ports={MYSQL_DEFAULT_PORT: port},
            labels={
                "zenml-test": "true",
            },
            extra_hosts={"host.docker.internal": "host-gateway"},
        )

        logging.info(
            f"Started container '{self.container_name}' "
            f"for deployment '{self.config.name}'."
        )

    def down(self) -> None:

        container = self.container
        if container is None:
            logging.info(
                f"Deployment '{self.config.name}' is no longer running. "
            )
            return

        while True:
            if container.status == "running":
                logging.info(
                    f"Stopping container '{self.container_name}' "
                    f"for deployment '{self.config.name}'."
                )
                container.stop()
            elif container.status == "exited":
                logging.info(
                    f"Removing container '{self.container_name}' "
                    f"for deployment '{self.config.name}'."
                )
                container.remove()
            time.sleep(1)
            container = self.container
            if container is None:
                break
        logging.info(f"Container '{self.container_name}' has been removed.")

    def get_store_config(self) -> Optional[DeploymentStoreConfig]:

        if not self.is_running:
            raise RuntimeError(
                f"The {self.config.name} deployment is not running."
            )

        container = self.container
        try:
            port = int(
                container.ports[f"{MYSQL_DEFAULT_PORT}/tcp"][0]["HostPort"]
            )
        except (KeyError, IndexError):
            raise RuntimeError(
                f"Could not find the port for the '{self.config.name}' "
                f"deployment."
            )

        return DeploymentStoreConfig(
            url=f"mysql://root:{MYSQL_DEFAULT_PASSWORD}@127.0.0.1:{port}/zenml"
        )


LocalDockerTestDeployment.register_deployment_class(
    type=DeploymentType.LOCAL, setup=DeploymentSetup.DOCKER
)
