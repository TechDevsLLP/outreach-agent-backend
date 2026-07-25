"""API tests: /api/auth — register, login, me, token handling, account gating.

NOTE: /api/auth/login is rate-limited to 10/minute per IP; the whole suite
must stay under that (currently 4 login calls here).
"""
import pytest

import database

pytestmark = pytest.mark.api


async def test_register_creates_user_account_membership(client):
    resp = await client.post("/api/auth/register", json={
        "email": "newuser@test.outflo.local",
        "password": "s3cret-pass",
        "name": "New User",
        "company_name": "NewCo",
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["token_type"] == "bearer"
    assert body["access_token"]

    user = await database.users_collection.find_one({"email": "newuser@test.outflo.local"})
    assert user is not None
    assert user["current_account_id"] is not None
    member = await database.account_members_collection.find_one({"user_id": user["_id"]})
    assert member is not None and member["role"] == "owner"

    # the returned token works
    me = await client.get("/api/auth/me",
                          headers={"Authorization": f"Bearer {body['access_token']}"})
    assert me.status_code == 200
    assert me.json()["user"]["email"] == "newuser@test.outflo.local"
    assert me.json()["account"]["name"] == "NewCo"


async def test_register_duplicate_email_409(client):
    payload = {"email": "dup@test.outflo.local", "password": "x1234567", "name": "Dup"}
    first = await client.post("/api/auth/register", json=payload)
    assert first.status_code == 201
    second = await client.post("/api/auth/register", json=payload)
    assert second.status_code == 409


async def test_login_success_and_wrong_password(client, identity_a):
    ok = await client.post("/api/auth/login", json={
        "email": identity_a["email"], "password": "test-password-123",
    })
    assert ok.status_code == 200
    assert ok.json()["access_token"]

    bad = await client.post("/api/auth/login", json={
        "email": identity_a["email"], "password": "wrong-password",
    })
    assert bad.status_code == 401


async def test_login_unknown_email_401(client):
    resp = await client.post("/api/auth/login", json={
        "email": "ghost@test.outflo.local", "password": "whatever123",
    })
    assert resp.status_code == 401


async def test_me_requires_auth(client):
    assert (await client.get("/api/auth/me")).status_code == 401


async def test_garbage_token_401(client):
    resp = await client.get("/api/auth/me",
                            headers={"Authorization": "Bearer not.a.jwt"})
    assert resp.status_code == 401


async def test_token_for_deleted_user_401(client):
    from auth import create_access_token
    from bson import ObjectId
    token = create_access_token({"sub": str(ObjectId()), "account_id": "", "email": "x@y.z"})
    resp = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


async def test_refresh_returns_new_token(client, auth_headers_a):
    resp = await client.post("/api/auth/refresh", headers=auth_headers_a)
    assert resp.status_code == 200
    assert resp.json()["access_token"]


async def test_patch_me_updates_profile(client, identity_a):
    resp = await client.patch("/api/auth/me", headers=identity_a["headers"],
                              json={"booking_link": "https://cal.test/usera"})
    assert resp.status_code == 200
    assert resp.json()["user"]["booking_link"] == "https://cal.test/usera"

    empty = await client.patch("/api/auth/me", headers=identity_a["headers"], json={})
    assert empty.status_code == 400


async def test_suspended_account_blocked_with_402(client, create_identity):
    suspended = await create_identity(
        "suspended@test.outflo.local", "Suspended User", "SuspendedCo",
        account_status="suspended",
    )
    resp = await client.get("/api/prospects", headers=suspended["headers"])
    assert resp.status_code == 402
    assert "suspended" in resp.json()["detail"].lower()


async def test_password_reset_full_flow(client):
    """Request a reset (dev-log path — SENDGRID_API_KEY is blanked in tests),
    read the token from the DB, confirm with a new password, then log in."""
    email = "resetme@test.outflo.local"
    reg = await client.post("/api/auth/register", json={
        "email": email, "password": "original-pass-1", "name": "Reset Me",
    })
    assert reg.status_code == 201

    req = await client.post("/api/auth/password-reset/request", json={"email": email})
    assert req.status_code == 200

    token_doc = await database.password_reset_tokens_collection.find_one({"email": email})
    assert token_doc is not None, "reset token was not persisted"

    confirm = await client.post("/api/auth/password-reset/confirm", json={
        "token": token_doc["token"], "new_password": "brand-new-pass-9",
    })
    assert confirm.status_code == 200, confirm.text

    # old password rejected, new password accepted
    old = await client.post("/api/auth/login", json={"email": email, "password": "original-pass-1"})
    assert old.status_code == 401
    new = await client.post("/api/auth/login", json={"email": email, "password": "brand-new-pass-9"})
    assert new.status_code == 200

    # token is single-use
    again = await client.post("/api/auth/password-reset/confirm", json={
        "token": token_doc["token"], "new_password": "another-pass-10",
    })
    assert again.status_code == 400


async def test_password_reset_short_password_400(client):
    resp = await client.post("/api/auth/password-reset/confirm", json={
        "token": "whatever", "new_password": "short",
    })
    assert resp.status_code == 400


async def test_password_reset_request_never_leaks_registration(client):
    resp = await client.post("/api/auth/password-reset/request",
                             json={"email": "not-registered@test.outflo.local"})
    assert resp.status_code == 200
