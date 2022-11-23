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

from abc import ABC, abstractmethod
from contextlib import contextmanager
import logging
import shutil
import sys
from docker.client import DockerClient

import os
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Generator, Optional, Tuple

from tests.harness.model import (
    DeploymentConfig,
    DeploymentSetup,
    DeploymentType,
    DeploymentStoreConfig,
)


if TYPE_CHECKING:
    from zenml.client import Client

LOCAL_ZENML_SERVER_DEFAULT_PORT = 9000
MYSQL_DOCKER_CONTAINER_NAME_PREFIX = "zenml-mysql-"
ZENML_SERVER_IMAGE_NAME = "localhost/zenml-server"
DEFAULT_DEPLOYMENT_ROOT_PATH = "/tmp/zenml-test"
ENV_DEPLOYMENT_ROOT_PATH = "ZENML_TEST_DEPLOYMENT_ROOT_PATH"


class BaseTestDeployment(ABC):

    DEPLOYMENTS: Dict[
        Tuple[DeploymentType, DeploymentSetup], "BaseTestDeployment"
    ] = {}

    def __init__(self, config: DeploymentConfig) -> None:
        self.config = config
        self._docker_client: Optional[DockerClient] = None

    @classmethod
    def register_deployment_class(
        cls, type: DeploymentType, setup: DeploymentSetup
    ) -> None:
        """Registers the deployment in the global registry."""

        BaseTestDeployment.DEPLOYMENTS[(type, setup)] = cls

    @classmethod
    def get_deployment_class(
        cls, type: DeploymentType, setup: DeploymentSetup
    ) -> Optional["BaseTestDeployment"]:
        """Returns the deployment class for the given type and setup.

        Args:
            type: The deployment type.
            setup: The deployment setup method.

        Returns:
            The deployment class registered for the given deployment type and
            setup method.
        """
        return cls.DEPLOYMENTS.get((type, setup))

    @classmethod
    def from_config(cls, config: DeploymentConfig) -> "BaseTestDeployment":
        """Creates a deployment from a deployment config.

        Args:
            config: The deployment config.

        Returns:
            The deployment instance.

        Raises:
            ValueError: If no deployment class is registered for the given
                deployment type and setup method.
        """
        deployment_class = cls.get_deployment_class(config.type, config.setup)
        if deployment_class is None:
            raise ValueError(
                f"No deployment class registered for type '{config.type}' "
                f"and setup '{config.setup}'"
            )
        return deployment_class(config)

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """Returns whether the deployment is running.

        Returns:
            Whether the deployment is running.
        """

    @abstractmethod
    def up(self) -> None:
        """Starts up the deployment."""

    @abstractmethod
    def down(self) -> None:
        """Tears down the deployment."""

    @abstractmethod
    def get_store_config(self) -> Optional[DeploymentStoreConfig]:
        """Returns the client store configuration needed to connect to the deployment.

        Returns:
           The store configuration, if one is required to connect to the
           deployment.
        """

    def cleanup(self) -> None:
        """Tears down the deployment and cleans up all local files."""
        self.down()

        config_path = self.global_config_path()
        if not config_path.exists():
            return

        if sys.platform == "win32":
            try:
                shutil.rmtree(config_path)
            except PermissionError:
                # Windows does not have the concept of unlinking a file and deleting
                #  once all processes that are accessing the resource are done
                #  instead windows tries to delete immediately and fails with a
                #  PermissionError: [WinError 32] The process cannot access the
                #  file because it is being used by another process
                logging.debug(
                    "Skipping deletion of temp dir at teardown, due to "
                    "Windows Permission error"
                )
                # TODO[HIGH]: Implement fixture cleanup for Windows where
                #  shutil.rmtree fails on files that are in use on python 3.7 and
                #  3.8
        else:
            shutil.rmtree(config_path)

    @property
    def docker_client(self) -> DockerClient:
        """Returns the docker client."""
        if self._docker_client is None:
            try:
                # Try to ping Docker, to see if it's installed and running
                docker_client = DockerClient.from_env()
                docker_client.ping()
                self._docker_client = docker_client
            except Exception as e:
                raise RuntimeError(
                    "Docker is not installed or running on this machine",
                    exc_info=True,
                ) from e

        return self._docker_client

    def _build_server_image(self) -> None:
        """Builds the server image locally."""

        from zenml.utils.docker_utils import build_image

        logging.info(
            f"Building ZenML server image '{ZENML_SERVER_IMAGE_NAME}' locally"
        )

        context_root = Path(__file__).parents[3]
        docker_file_path = (
            context_root / "docker" / "zenml-server-dev.Dockerfile"
        )
        build_image(
            image_name=ZENML_SERVER_IMAGE_NAME,
            dockerfile=str(docker_file_path),
            build_context_root=str(context_root),
        )

    @classmethod
    def get_root_path(cls) -> Path:
        """Returns the root path used for test deployments.

        Returns:
            The root path for test deployments.
        """
        if ENV_DEPLOYMENT_ROOT_PATH in os.environ:
            return Path(os.environ[ENV_DEPLOYMENT_ROOT_PATH])

        return Path(DEFAULT_DEPLOYMENT_ROOT_PATH)

    def global_config_path(self) -> Path:
        """Returns the global config path used for the deployment."""

        return self.get_root_path() / self.config.name

    @contextmanager
    def connect(
        self,
        global_config_path: Optional[Path] = None,
    ) -> Generator["Client", None, None]:
        """Context manager to create a client and connect it to the deployment.

        Call this method to configure zenml to connect to this deployment,
        run some code in the context of this configuration and then
        switch back to the previous configuration.

        Args:
            global_config_path: Custom global config path. If not provided,
                the global config path where the deployment is provisioned
                is used.

        Yields:
            A ZenML Client configured to connect to this deployment.
        """
        from zenml.config.global_config import GlobalConfiguration
        from zenml.config.store_config import StoreConfiguration
        from zenml.zen_stores.base_zen_store import BaseZenStore

        from zenml.client import Client

        # set the ZENML_CONFIG_PATH environment variable to ensure that the
        # deployment uses a config isolated from the main config
        config_path = global_config_path or self.global_config_path()
        if not config_path.exists():
            config_path.mkdir(parents=True)

        # save the current global configuration and repository singleton instances
        # to restore them later, then reset them
        original_config = GlobalConfiguration.get_instance()
        original_client = Client.get_instance()
        orig_config_path = os.getenv("ZENML_CONFIG_PATH")

        GlobalConfiguration._reset_instance()
        Client._reset_instance()

        os.environ["ZENML_CONFIG_PATH"] = str(config_path)
        os.environ["ZENML_ANALYTICS_OPT_IN"] = "false"

        # initialize the global config and client at the new path
        gc = GlobalConfiguration()
        gc.analytics_opt_in = False

        store_config = self.get_store_config()
        if store_config is not None:
            gc.store = StoreConfiguration(
                type=BaseZenStore.get_store_type(store_config.url),
                **store_config.dict(),
            )
        client = Client()

        yield client

        # restore the global configuration path
        if orig_config_path:
            os.environ["ZENML_CONFIG_PATH"] = orig_config_path
        else:
            del os.environ["ZENML_CONFIG_PATH"]

        # restore the global configuration and the client
        GlobalConfiguration._reset_instance(original_config)
        Client._reset_instance(original_client)
