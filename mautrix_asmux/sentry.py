import logging

from sentry_sdk.integrations.aiohttp import AioHttpIntegration
import sentry_sdk

log = logging.getLogger("mau.segment")


def init(environment: str, dsn: str):
    log.debug("Initializing Sentry for environment: %s", environment)

    sentry_sdk.init(
        environment=environment,
        dsn=dsn,
        integrations=[AioHttpIntegration()],
    )
