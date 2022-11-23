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

import sys
from typing import List
import click
from tests.harness.harness import TestHarness
from tests.harness.cli.cli import cli


@cli.group
def environment() -> None:
    """View and manage test environments."""


@environment.command("list")
def list_environment() -> None:
    """List configured environment."""
    from zenml.cli.utils import print_table

    harness = TestHarness()
    environments = []
    for environment in harness.environments.values():
        environment_cfg = environment.config
        values = dict(
            name=environment_cfg.name,
            deployment=environment_cfg.deployment.name,
            requirements=", ".join(
                [r.name for r in environment_cfg.requirements if r.name]
            ),
        )
        is_running = environment.is_running
        is_provisioned = is_running and environment.is_provisioned
        values["running"] = ":white_check_mark:" if is_running else ":x:"
        values["provisioned"] = (
            ":white_check_mark:" if is_provisioned else ":x:"
        )
        values["url"] = ""
        if is_running:
            store_cfg = environment.deployment.get_store_config()
            if store_cfg:
                values["url"] = store_cfg.url

        environments.append(values)

    print_table(environments)


@environment.command("up")
@click.argument("name", type=str, required=True)
def start_environment(name: str) -> None:
    """Start a configured environment."""
    harness = TestHarness()
    environment = harness.get_environment(name)
    environment.up()
    store_cfg = environment.deployment.get_store_config()
    url = f" at {store_cfg.url}" if store_cfg else ""
    print(f"Environment '{name}' is running{url}.")


@environment.command("down")
@click.argument("name", type=str, required=True)
def stop_environment(name: str) -> None:
    """Deprovision and stop a configured environment."""
    harness = TestHarness()
    environment = harness.get_environment(name)
    environment.down()


@environment.command("provision")
@click.argument("name", type=str, required=True)
def provision_environment(name: str) -> None:
    """Provision a configured environment."""
    harness = TestHarness()
    environment = harness.get_environment(name)
    environment.provision()
    store_cfg = environment.deployment.get_store_config()
    url = f" at {store_cfg.url}" if store_cfg else ""
    print(f"Environment '{name}' is provisioned and running{url}.")


@environment.command("deprovision")
@click.argument("name", type=str, required=True)
def deprovision_environment(name: str) -> None:
    """Deprovision a configured environment."""
    harness = TestHarness()
    environment = harness.get_environment(name)
    environment.deprovision()


@environment.command("cleanup")
@click.argument("name", type=str, required=True)
def cleanup_environment(name: str) -> None:
    """Deprovision, stop a configured environment and clean up all the local files."""
    harness = TestHarness()
    environment = harness.get_environment(name)
    environment.cleanup()
