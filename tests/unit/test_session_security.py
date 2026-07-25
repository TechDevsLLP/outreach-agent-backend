"""Focused browser-session, CSRF, and bearer-compatibility regression tests."""

import inspect
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from bson import ObjectId
from fastapi import HTTPException, Response
from starlette.requests import Request

import auth
from routes import auth as auth_routes
from routes import notifications

pytestmark = pytest.mark.unit


def _request(
    method: str = "GET",
    *,
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> Request:
    raw_headers = [(key.lower().encode(), value.encode()) for key, value in (headers or {}).items()]
    if cookies:
        raw_headers.append(
            (b"cookie", "; ".join(f"{key}={value}" for key, value in cookies.items()).encode())
        )
    return Request(
        {
            "type": "http",
            "method": method,
            "path": "/api/test",
            "headers": raw_headers,
            "query_string": b"",
            "scheme": "https",
            "server": ("testserver", 443),
            "client": ("127.0.0.1", 1),
        }
    )


def test_browser_auth_response_sets_httponly_session_without_returning_jwt():
    response = Response()
    request = _request(headers={"X-OutFlo-Client": "browser"})

    payload = auth_routes._complete_authentication(request, response, "signed.jwt.value")

    assert payload == {"authenticated": True}
    set_cookie_headers = response.headers.getlist("set-cookie")
    session = next(value for value in set_cookie_headers if value.startswith("auth_token="))
    csrf = next(value for value in set_cookie_headers if value.startswith("outflo_csrf="))
    assert "HttpOnly" in session
    assert "SameSite=lax" in session
    assert "HttpOnly" not in csrf
    assert "signed.jwt.value" not in repr(payload)


def test_non_browser_auth_contract_remains_bearer_compatible():
    response = Response()
    payload = auth_routes._complete_authentication(_request(), response, "api.jwt")

    assert payload == {"access_token": "api.jwt", "token_type": "bearer"}
    assert response.headers.get("set-cookie") is None


def test_cookie_mutation_requires_matching_double_submit_token():
    with pytest.raises(HTTPException) as exc:
        auth.validate_cookie_csrf(
            _request("POST", cookies={auth.CSRF_COOKIE_NAME: "cookie-value"})
        )
    assert exc.value.status_code == 403

    auth.validate_cookie_csrf(
        _request(
            "POST",
            headers={auth.CSRF_HEADER_NAME: "same-value"},
            cookies={auth.CSRF_COOKIE_NAME: "same-value"},
        )
    )


def test_production_cookie_mutation_requires_exact_allowed_origin(monkeypatch):
    monkeypatch.setattr(auth.settings, "app_env", "production")
    monkeypatch.setattr(auth.settings, "cors_origins", "https://app.outflo.test")
    cookies = {auth.CSRF_COOKIE_NAME: "csrf"}
    base_headers = {auth.CSRF_HEADER_NAME: "csrf"}

    with pytest.raises(HTTPException):
        auth.validate_cookie_csrf(_request("PATCH", headers=base_headers, cookies=cookies))
    with pytest.raises(HTTPException):
        auth.validate_cookie_csrf(
            _request(
                "PATCH",
                headers={**base_headers, "Origin": "https://evil.example"},
                cookies=cookies,
            )
        )

    auth.validate_cookie_csrf(
        _request(
            "PATCH",
            headers={**base_headers, "Origin": "https://app.outflo.test"},
            cookies=cookies,
        )
    )


async def test_explicit_bearer_client_does_not_require_csrf(monkeypatch):
    user_id = ObjectId()

    class _Users:
        async def find_one(self, query):
            assert query == {"_id": user_id}
            return {"_id": user_id, "email": "api@example.test"}

    monkeypatch.setitem(sys.modules, "database", SimpleNamespace(users_collection=_Users()))
    token = auth.create_access_token({"sub": str(user_id)})

    user = await auth.get_current_user(_request("POST"), bearer_token=token)

    assert user["_id"] == str(user_id)


def test_notification_stream_has_no_query_string_token_contract():
    parameters = inspect.signature(notifications.notification_stream).parameters
    assert "token" not in parameters
    assert "account_ctx" in parameters


def test_token_rotation_preserves_impersonation_binding_and_deadline():
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=12)
    token = auth.rotate_user_access_token(
        {
            "_id": str(ObjectId()),
            "email": "target@example.test",
            "_impersonated_by": str(ObjectId()),
            "_session_expires_at": expires_at.timestamp(),
        },
        str(ObjectId()),
    )
    payload = auth.decode_access_token(token)

    assert payload["impersonated_by"]
    assert payload["exp"] <= int(expires_at.timestamp()) + 1
