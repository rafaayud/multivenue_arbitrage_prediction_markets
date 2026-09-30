"""Send mobile push notifications through the FCM HTTP v1 API.

Responsibilities
----------------
- Exchange a service-account assertion for a short-lived OAuth access token.
- Submit notification and data payloads to one FCM device token.
"""

import base64
import json
from pathlib import Path
import time
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
import httpx

_FCM_SCOPE = "https://www.googleapis.com/auth/firebase.messaging"
_JWT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"


def _base64url(value: bytes) -> bytes:
    """Encode bytes for a compact JWT without padding."""
    return base64.urlsafe_b64encode(value).rstrip(b"=")


class FcmPushClient:
    """Send pushes with one Firebase service account.

    Parameters
    ----------
    project_id
        Firebase project receiving the message request.
    client_email
        Issuer email from the Firebase service-account file.
    private_key
        PEM-encoded RSA private key used to sign OAuth assertions.
    token_uri
        Google OAuth token endpoint from the service-account file.
    private_key_id
        Optional key identifier included in the JWT header.
    timeout_seconds
        Maximum HTTP and total webhook duration. It must remain below the
        five-second Alertmanager webhook timeout.
    client
        Optional caller-owned HTTP client, primarily for tests.

    Notes
    -----
    - Access tokens are cached until one minute before their reported expiry.
    """

    def __init__(
        self,
        *,
        project_id: str,
        client_email: str,
        private_key: str,
        token_uri: str,
        private_key_id: str | None = None,
        timeout_seconds: float = 4.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not 0 < timeout_seconds < 5:
            raise ValueError("timeout_seconds must be greater than 0 and below 5")
        key = serialization.load_pem_private_key(private_key.encode(), password=None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise ValueError("FCM service-account private_key must be RSA")

        self.project_id = project_id
        self.client_email = client_email
        self.token_uri = token_uri
        self.private_key_id = private_key_id
        self.timeout_seconds = timeout_seconds
        self._private_key = key
        self._own_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds)
        self._access_token: str | None = None
        self._access_token_expires_at = 0.0

    @classmethod
    def from_service_account_file(
        cls,
        path: str | Path,
        *,
        timeout_seconds: float = 4.0,
        client: httpx.AsyncClient | None = None,
    ) -> "FcmPushClient":
        """Build a client from a Firebase service-account JSON file.

        Parameters
        ----------
        path
            Path to the JSON credential file. The file must remain outside
            source control.
        timeout_seconds
            HTTP and webhook timeout, strictly lower than five seconds.
        client
            Optional caller-owned HTTP client.

        Returns
        -------
        FcmPushClient
            Configured FCM HTTP v1 client.

        Raises
        ------
        OSError
            If the credential file cannot be read.
        ValueError
            If the file is malformed or lacks required service-account fields.
        """
        try:
            credentials = json.loads(Path(path).read_text(encoding="utf-8"))
            if (
                not isinstance(credentials, dict)
                or credentials.get("type") != "service_account"
            ):
                raise ValueError("FCM credentials must be a service account")
            required = ("project_id", "client_email", "private_key", "token_uri")
            if not all(
                isinstance(credentials.get(field), str) and credentials[field]
                for field in required
            ):
                raise ValueError("Invalid FCM service-account file")
            return cls(
                project_id=credentials["project_id"],
                client_email=credentials["client_email"],
                private_key=credentials["private_key"],
                token_uri=credentials["token_uri"],
                private_key_id=credentials.get("private_key_id"),
                timeout_seconds=timeout_seconds,
                client=client,
            )
        except (KeyError, TypeError, json.JSONDecodeError) as error:
            raise ValueError("Invalid FCM service-account file") from error

    async def close(self) -> None:
        """Close the internally owned HTTP client."""
        if self._own_client:
            await self._client.aclose()

    async def send(
        self,
        *,
        token: str,
        title: str,
        body: str,
        data: dict[str, str],
    ) -> str:
        """Send one push to an FCM device registration token.

        Parameters
        ----------
        token
            FCM registration token resolved from the configured recipient ID.
        title
            Visible notification title.
        body
            Visible notification body.
        data
            String-valued metadata delivered to the mobile application.

        Returns
        -------
        str
            Provider-assigned FCM message name.

        Raises
        ------
        httpx.HTTPStatusError
            If OAuth or FCM rejects the request.
        httpx.TransportError
            If either provider endpoint is unavailable.
        ValueError
            If a successful provider response lacks the expected value.
        """
        access_token = await self._get_access_token()
        response = await self._client.post(
            f"https://fcm.googleapis.com/v1/projects/{self.project_id}/messages:send",
            headers={"Authorization": f"Bearer {access_token}"},
            json={
                "message": {
                    "token": token,
                    "notification": {"title": title, "body": body},
                    "data": data,
                }
            },
        )
        response.raise_for_status()
        try:
            name = response.json()["name"]
        except (KeyError, TypeError, json.JSONDecodeError) as error:
            raise ValueError("FCM response lacks a message name") from error
        if not isinstance(name, str) or not name:
            raise ValueError("FCM response lacks a message name")
        return name

    async def _get_access_token(self) -> str:
        """Return a cached access token or exchange a new signed assertion."""
        now = time.time()
        if self._access_token and now < self._access_token_expires_at - 60:
            return self._access_token

        response = await self._client.post(
            self.token_uri,
            data={
                "grant_type": _JWT_GRANT_TYPE,
                "assertion": self._service_account_assertion(int(now)),
            },
        )
        response.raise_for_status()
        try:
            payload: dict[str, Any] = response.json()
            access_token = payload["access_token"]
            expires_in = float(payload.get("expires_in", 3600))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("OAuth response lacks a valid access token") from error
        if not isinstance(access_token, str) or not access_token:
            raise ValueError("OAuth response lacks a valid access token")

        self._access_token = access_token
        self._access_token_expires_at = now + expires_in
        return access_token

    def _service_account_assertion(self, issued_at: int) -> str:
        """Create the RS256 OAuth service-account assertion."""
        header = {"alg": "RS256", "typ": "JWT"}
        if self.private_key_id:
            header["kid"] = self.private_key_id
        claims = {
            "iss": self.client_email,
            "scope": _FCM_SCOPE,
            "aud": self.token_uri,
            "iat": issued_at,
            "exp": issued_at + 3600,
        }
        encoded_header = _base64url(
            json.dumps(header, separators=(",", ":")).encode()
        )
        encoded_claims = _base64url(
            json.dumps(claims, separators=(",", ":")).encode()
        )
        signing_input = encoded_header + b"." + encoded_claims
        signature = self._private_key.sign(
            signing_input,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return b".".join((signing_input, _base64url(signature))).decode()
