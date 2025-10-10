# -*- coding: utf-8 -*-
# pylint: disable=protected-access, unused-argument
import asyncio
import inspect
import logging

from typing import Optional, Callable

import websockets
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware

from ...manager.server.config import get_settings
from ...manager.server.models import (
    ErrorResponse,
    HealthResponse,
)
from ...manager.sandbox_manager import SandboxManager
from ...model.manager_config import SandboxManagerEnvConfig
from ....version import __version__

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UploadTriggerMiddleware(BaseHTTPMiddleware):
    """
    Middleware to automatically trigger uploads after specific endpoints are called.
    This middleware intercepts responses and triggers uploads for code execution endpoints.
    """

    # List of endpoints that should trigger uploads after execution
    UPLOAD_TRIGGER_ENDPOINTS = {
        "/call_tool": "POST"
    }

    async def dispatch(self, request: Request, call_next: Callable):
        # Check if this is an endpoint that should trigger an upload
        # We need to check this before calling call_next to be able to read the body
        should_trigger_upload = (request.url.path in self.UPLOAD_TRIGGER_ENDPOINTS and
                                 request.method == self.UPLOAD_TRIGGER_ENDPOINTS[request.url.path])

        identity = None
        if should_trigger_upload:
            try:
                # Extract identity from the request body
                body = await request.body()
                if body:
                    import json
                    try:
                        data = json.loads(body.decode())
                        identity = data.get('identity')
                    except json.JSONDecodeError:
                        logger.error("Failed to decode JSON body")

                # Create a new request with the same body for the next middleware/handler
                async def receive() -> dict:
                    return {"type": "http.request", "body": body, "more_body": False}

                request = Request(request.scope, receive=receive)
            except Exception as e:
                logger.error(f"Error extracting identity from request: {e}")

        # Process the request
        response = await call_next(request)

        # 打印request的所有请求参数
        logger.info(f"Response status code: {request.url.path}")
        logger.info(f"Response body: {request.method}")

        # Trigger upload after the request is processed
        if should_trigger_upload and identity:
            logger.debug(f"Upload trigger endpoint hit: {request.url.path}")
            try:
                # Trigger upload in background task
                asyncio.create_task(self._trigger_upload_async(identity))
            except Exception as e:
                logger.error(f"Error setting up upload trigger: {e}")

        return response

    async def _trigger_upload_async(self, container_id: str):
        """
        Trigger upload asynchronously without blocking the response.
        """
        try:
            sandbox_manager = get_sandbox_manager()
            logger.debug(f"Triggering upload for container {container_id}")
            result = sandbox_manager.upload_on_completion(container_id)
            logger.info(f"Upload triggered for container {container_id}, result: {result}")
        except Exception as e:
            logger.error(f"Error triggering upload for container {container_id}: {e}")


# Create FastAPI app
app = FastAPI(
    title="Runtime Manager Service",
    description="Service for managing runtime containers",
    version=__version__,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(UploadTriggerMiddleware)
# Security scheme
security = HTTPBearer(auto_error=False)

# Global SandboxManager instance
_sandbox_manager: Optional[SandboxManager] = None
_config: Optional[SandboxManagerEnvConfig] = None


def get_config() -> SandboxManagerEnvConfig:
    """Return config"""
    global _config
    if _config is None:
        settings = get_settings()
        _config = SandboxManagerEnvConfig(
            container_prefix_key=settings.CONTAINER_PREFIX_KEY,
            file_system=settings.FILE_SYSTEM,
            redis_enabled=settings.REDIS_ENABLED,
            container_deployment=settings.CONTAINER_DEPLOYMENT,
            default_mount_dir=settings.DEFAULT_MOUNT_DIR,
            storage_folder=settings.STORAGE_FOLDER,
            port_range=settings.PORT_RANGE,
            pool_size=settings.POOL_SIZE,
            oss_endpoint=settings.OSS_ENDPOINT,
            oss_access_key_id=settings.OSS_ACCESS_KEY_ID,
            oss_access_key_secret=settings.OSS_ACCESS_KEY_SECRET,
            oss_bucket_name=settings.OSS_BUCKET_NAME,
            redis_server=settings.REDIS_SERVER,
            redis_port=settings.REDIS_PORT,
            redis_db=settings.REDIS_DB,
            redis_user=settings.REDIS_USER,
            redis_password=settings.REDIS_PASSWORD,
            redis_port_key=settings.REDIS_PORT_KEY,
            redis_container_pool_key=settings.REDIS_CONTAINER_POOL_KEY,
            k8s_namespace=settings.K8S_NAMESPACE,
            kubeconfig_path=settings.KUBECONFIG_PATH,
            s3_endpoint_url=settings.S3_ENDPOINT_URL,
            s3_access_key_id=settings.S3_ACCESS_KEY_ID,
            s3_access_key_secret=settings.S3_ACCESS_KEY_SECRET,
            s3_bucket_name=settings.S3_BUCKET_NAME,
            s3_region_name=settings.S3_REGION_NAME,
        )
    return _config


def verify_token(
        credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Verify Bearer token"""
    settings = get_settings()

    if not hasattr(settings, "BEARER_TOKEN") or not settings.BEARER_TOKEN:
        logger.warning("BEARER_TOKEN not configured, skipping authentication")
        return credentials

    if credentials is None:
        logger.error("Authentication required but no token provided")
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if credentials.credentials != settings.BEARER_TOKEN:
        logger.error(
            f"Invalid token provided: {credentials.credentials[:10]}...",
        )
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials


def get_sandbox_manager():
    """Get or create the global SandboxManager instance"""
    global _sandbox_manager
    if _sandbox_manager is None:
        settings = get_settings()
        config = get_config()
        _sandbox_manager = SandboxManager(
            config=config,
            default_type=settings.DEFAULT_SANDBOX_TYPE,
        )
    return _sandbox_manager


def create_endpoint(method):
    async def endpoint(
            request: Request,
            token: HTTPAuthorizationCredentials = Depends(verify_token),
    ):
        try:
            data = await request.json()
            logger.info(
                f"Calling {method.__name__} with data: {data}",
            )
            result = method(**data)
            if hasattr(result, "model_dump_json"):
                return JSONResponse(content={"data": result.model_dump_json()})
            return JSONResponse(content={"data": result})
        except Exception as e:
            error = (
                f"Error in {method.__name__}: {str(e)},"
                # f" {traceback.format_exc()}"
            )
            logger.error(error)
            raise HTTPException(status_code=500, detail=error) from e

    return endpoint


def register_routes(_app, instance):
    for _, method in inspect.getmembers(
            instance,
            predicate=inspect.ismethod,
    ):
        if getattr(method, "_is_remote_wrapper", False):
            http_method = method._http_method.lower()
            path = method._path

            endpoint = create_endpoint(method)

            if http_method == "get":
                _app.get(path)(endpoint)
            elif http_method == "post":
                _app.post(path)(endpoint)
            elif http_method == "delete":
                _app.delete(path)(endpoint)


@app.on_event("startup")
async def startup_event():
    """Initialize the SandboxManager on startup"""
    get_sandbox_manager()
    register_routes(app, _sandbox_manager)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on shutdown"""
    global _sandbox_manager
    settings = get_settings()
    if _sandbox_manager and settings.AUTO_CLEANUP:
        _sandbox_manager.cleanup()
        _sandbox_manager = None


@app.get(
    "/health",
    response_model=HealthResponse,
    responses={500: {"model": ErrorResponse}},
)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        version=_sandbox_manager.default_type.value,
    )


@app.websocket("/browser/{sandbox_id}/cast")
async def websocket_endpoint(
        websocket: WebSocket,
        sandbox_id: str,
):
    global _sandbox_manager

    await websocket.accept()

    container_json = _sandbox_manager.container_mapping.get(
        sandbox_id,
    )
    service_address = None
    if container_json:
        service_address = container_json.get("front_browser_ws")

    logger.debug(f"service_address: {service_address}")

    if not service_address:
        await websocket.close(code=1001)
        return

    try:
        query_params = websocket.query_params
        target_url = service_address
        if query_params:
            query_string = str(query_params)
            if "?" in target_url:
                target_url += "&" + query_string
            else:
                target_url += "?" + query_string

        logger.info(f"Connecting to target with URL: {target_url}")

        # Connect to the target WebSocket server
        async with websockets.connect(target_url) as target_ws:
            # Forward messages from client to target server
            async def forward_to_service():
                try:
                    async for message in websocket.iter_text():
                        await target_ws.send(message)
                except WebSocketDisconnect:
                    logger.debug(
                        f"WebSocket disconnected from client for sandbox"
                        f" {sandbox_id}",
                    )
                    await target_ws.close()

            # Forward messages from target server to client
            async def forward_to_client():
                try:
                    async for message in target_ws:
                        await websocket.send_text(message)
                except websockets.exceptions.ConnectionClosed:
                    logger.debug(
                        f"WebSocket disconnected from service for sandbox"
                        f" {sandbox_id}",
                    )
                    await websocket.close()

            # Run both tasks concurrently
            await asyncio.gather(forward_to_service(), forward_to_client())

    except Exception as e:
        logger.error(f"Error in sandbox {sandbox_id}: {e}")
        await websocket.close()


# TODO: add socketio relay endpoint for filesystem


def setup_logging(log_level: str):
    """Setup logging configuration based on log level"""
    # Convert string to logging level
    level_mapping = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    level = level_mapping.get(log_level.upper(), logging.INFO)

    # Reconfigure logging
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,  # This will reconfigure existing loggers
    )

    # Update the logger for this module
    global logger
    logger.setLevel(level)

    logger.info(f"Logging level set to {log_level.upper()}")


def main():
    """Main entry point for the Runtime Manager Service"""
    import argparse
    import os
    import uvicorn

    parser = argparse.ArgumentParser(description="Runtime Manager Service")
    parser.add_argument("--config", type=str, help="Path to config file")
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()

    # Setup logging based on command line argument
    setup_logging(args.log_level)

    if args.config and not os.path.exists(args.config):
        raise FileNotFoundError(
            f"Error: Config file {args.config} does not exist",
        )

    settings = get_settings(args.config)

    uvicorn.run(
        "agentscope_runtime.sandbox.manager.server.app:app",
        host=settings.HOST,
        port=settings.PORT,
        workers=settings.WORKERS,
        reload=settings.DEBUG,
    )


if __name__ == "__main__":
    main()
