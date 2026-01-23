# burningdemand_dagster/resources/http_clients_resource.py
import asyncio
import httpx
from dagster import ConfigurableResource
from pydantic import Field


def _safe_async_close(coro):
    """
    Close an async resource from a sync context without crashing if a loop is already running.
    """
    try:
        loop = asyncio.get_running_loop()
        # If we're in an event loop already, schedule and return.
        loop.create_task(coro)
    except RuntimeError:
        # No running loop -> safe to run directly.
        asyncio.run(coro)


class HTTPClientsResource(ConfigurableResource):
    user_agent: str = Field(default="BurningDemand/0.1")

    def setup_for_execution(self, context) -> None:
        self._client = httpx.Client(
            timeout=httpx.Timeout(30.0),
            headers={"User-Agent": self.user_agent},
        )
        self._aclient = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers={"User-Agent": self.user_agent},
        )

    def teardown_after_execution(self, context) -> None:
        try:
            self._client.close()
        except Exception:
            pass
        try:
            _safe_async_close(self._aclient.aclose())
        except Exception:
            pass

    @property
    def client(self) -> httpx.Client:
        return self._client

    @property
    def aclient(self) -> httpx.AsyncClient:
        return self._aclient
