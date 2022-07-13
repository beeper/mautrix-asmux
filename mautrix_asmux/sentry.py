from sentry_sdk.integrations.aiohttp import AioHttpIntegration
import sentry_sdk


def init(environment: str, dsn: str):
    sentry_sdk.init(
        environment=environment,
        dsn=dsn,
        integrations=[AioHttpIntegration()],
    )
