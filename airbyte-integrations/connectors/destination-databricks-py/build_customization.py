#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dagger import Container


def resolve_connector_secret_mount_path() -> Path:
    """
    Secret mount path looks like '/airbyte-integrations/connectors/{CONNECTOR_NAME}/secrets/{SECRET_FILES}'
    """
    connector_name = Path(__file__).parent.name
    return Path('/', 'airbyte-integrations', 'connectors', connector_name, 'secrets')


async def pre_connector_install(base_image_container: Container) -> Container:
    """
    Set NETRC env variable to access private pypi repository
    """
    assert (Path(__file__).parent / 'secrets' / '.netrc').exists(), 'Could not find .netrc secret'
    container_netrc_path = str(resolve_connector_secret_mount_path() / '.netrc')
    return (
        base_image_container
#         .with_exec(["sh", "-c", "apt-get update && apt-get install -y netcat-traditional host"], skip_entrypoint=True)
#         .with_exec(["nc", "-v", "-z", "13.107.42.20", "443"], skip_entrypoint=True)
#         .with_exec(["host", "pkgs.dev.azure.com"], skip_entrypoint=True)
#         .with_exec(["nc", "-v", "-z", "pkgs.dev.azure.com", "443"], skip_entrypoint=True)
        .with_env_variable('NETRC', container_netrc_path)
#         .with_exec(["ls", container_netrc_path], skip_entrypoint=True,)
#         .with_exec(["mkdir", "-p", Path('~').expanduser().absolute()], skip_entrypoint=True,)
#         .with_exec(["cp", container_netrc_path, (Path('~') / '.netrc').expanduser().absolute()], skip_entrypoint=True,)
    )
