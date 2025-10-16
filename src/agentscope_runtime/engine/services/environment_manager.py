# -*- coding: utf-8 -*-
from typing import List
from contextlib import asynccontextmanager

from .manager import ServiceManager
from .sandbox_service import SandboxService


class EnvironmentManager(ServiceManager):
    """
    The EnvironmentManager class for managing environment-related services.
    """

    def __init__(self, sandbox_service: SandboxService = None):
        self._sandbox_service = sandbox_service
        super().__init__()

    def _register_default_services(self):
        """Register default services for environment management."""
        self._sandbox_service = self._sandbox_service or SandboxService()
        self.register_service("sandbox", self._sandbox_service)

    def connect_sandbox(
        self,
        session_id,
        user_id,
        env_types=None,
        tools=None,
    ) -> List:

        sandboxes =  self._sandbox_service.connect(
            session_id,
            user_id,
            env_types=env_types,
            tools=tools,
        )
        composite_key = self._sandbox_service._create_composite_key(session_id, user_id)
        env_ids = self._sandbox_service.session_mapping.get(composite_key, [])

    def release_sandbox(self, session_id, user_id):
        return self._sandbox_service.release(session_id, user_id)


@asynccontextmanager
async def create_environment_manager(
    sandbox_service: SandboxService = None,
):
    manager = EnvironmentManager(
        sandbox_service=sandbox_service,
    )

    async with manager:
        yield manager
