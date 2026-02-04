import os
from typing import Any, Dict, Optional, Tuple

import requests
from dagster import ConfigurableResource


class PocketBaseResource(ConfigurableResource):
    """
    Dagster resource for PocketBase client.
    Provides HTTP API access to PocketBase collections with authentication.
    """

    base_url: str
    email: str
    password: str
    timeout_s: int = 20

    def setup_for_execution(self, context) -> None:
        """Initialize PocketBase connection and authenticate."""
        self._base_url = self.base_url.rstrip("/")
        self._token: Optional[str] = None
        self._user_id: Optional[str] = None
        self._login()

    @property
    def user_id(self) -> str:
        """Authenticated user's record id (for reporter, etc.). Available after setup_for_execution."""
        if self._user_id is None:
            raise RuntimeError("PocketBase not authenticated yet (user_id unavailable).")
        return self._user_id

    def _headers(self) -> Dict[str, str]:
        """Get HTTP headers with authentication token."""
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _login(self) -> None:
        """Authenticate with PocketBase and store token."""
        url = f"{self._base_url}/api/collections/users/auth-with-password"
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"identity": self.email, "password": self.password},
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("token")
        if not token:
            raise RuntimeError("PocketBase auth succeeded but no token returned.")
        self._token = token
        record = data.get("record") or {}
        user_id = record.get("id")
        if not user_id:
            raise RuntimeError("PocketBase auth succeeded but no record.id returned.")
        self._user_id = user_id

    def _ensure_auth(self) -> None:
        """Ensure we have a valid authentication token."""
        if not self._token:
            self._login()

    def get_one_by_filter(
        self, collection: str, filter_expr: str
    ) -> Optional[Dict[str, Any]]:
        """Get a single record from a collection matching the filter."""
        self._ensure_auth()
        url = f"{self._base_url}/api/collections/{collection}/records"
        resp = requests.get(
            url,
            headers=self._headers(),
            params={"filter": filter_expr, "perPage": 1, "page": 1},
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        return items[0] if items else None

    def create(self, collection: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new record in a collection."""
        self._ensure_auth()
        url = f"{self._base_url}/api/collections/{collection}/records"
        resp = requests.post(
            url, headers=self._headers(), json=payload, timeout=self.timeout_s
        )
        resp.raise_for_status()
        return resp.json()

    def update(
        self, collection: str, record_id: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an existing record in a collection."""
        self._ensure_auth()
        url = f"{self._base_url}/api/collections/{collection}/records/{record_id}"
        resp = requests.patch(
            url, headers=self._headers(), json=payload, timeout=self.timeout_s
        )
        resp.raise_for_status()
        return resp.json()

    def upsert_ing_item(self, item: Dict[str, Any]) -> Tuple[str, str]:
        """
        Upsert into ing_items by (source, source_id).
        Returns (action, record_id) where action in {"created","updated","skipped"}.
        """
        source = item["source"]
        source_id = str(item["source_id"]).replace(
            '"', '\\"'
        )  # naive escape for PB filter

        # If you have a unique index, this is still safest for now (simple).
        existing = self.get_one_by_filter(
            "ing_items",
            f'source="{source}" && source_id="{source_id}"',
        )

        # Skip update if content_hash unchanged
        if (
            existing
            and existing.get("content_hash")
            and existing.get("content_hash") == item.get("content_hash")
        ):
            return ("skipped", existing["id"])

        if not existing:
            rec = self.create("ing_items", item)
            return ("created", rec["id"])

        rec = self.update("ing_items", existing["id"], item)
        return ("updated", rec["id"])

    @classmethod
    def from_env(cls) -> "PocketBaseResource":
        """Create a PocketBaseResource from environment variables."""
        pb_url = os.getenv("PB_URL", "http://127.0.0.1:8090")
        pb_email = os.getenv("PB_EMAIL")
        pb_password = os.getenv("PB_PASSWORD")

        if not pb_email or not pb_password:
            raise RuntimeError("Missing PB_EMAIL / PB_PASSWORD env vars.")

        return cls(
            base_url=pb_url,
            email=pb_email,
            password=pb_password,
        )
