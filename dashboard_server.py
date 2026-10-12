# -*- coding: utf-8 -*-
"""
FreeFire Level Up Bot - Dashboard Server
Firebase Auth + Per-User Isolation + Admin Panel + Realtime DB Sync
"""

import asyncio
import json
import os
import time
import httpx
from typing import Dict, List, Any, Optional, Tuple
from aiohttp import web

# ==================== FIREBASE CONFIG ====================
FIREBASE_CONFIG = {
    "apiKey": "AIzaSyD3XuX0-zuV0nbiZAjY7mnqNBvyshh-cns",
    "authDomain": "trust--tournament.firebaseapp.com",
    "databaseURL": "https://trust--tournament-default-rtdb.firebaseio.com",
    "projectId": "trust--tournament",
    "storageBucket": "trust--tournament.appspot.com",
    "messagingSenderId": "952470695005",
    "appId": "1:952470695005:web:4e0fe64b9b6afcfb8a71b4",
    "measurementId": "G-WS83KRQQYC"
}

ADMIN_EMAILS = set(e.strip().lower() for e in os.environ.get("ADMIN_EMAILS", "").split(",") if e.strip())

# ==================== BOT STATE ====================
class BotState:
    def __init__(self):
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: List[Dict[str, Any]] = []
        self.max_logs = 800
        self.total_matches = 0
        self.total_gained_exp = 0
        self.start_time = time.time()
        self.account_workers: Dict[str, asyncio.Task] = {}
        self.refresh_callbacks: Dict[str, Any] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}
        # auth_key -> owner firebase uid
        self.account_owners: Dict[str, str] = {}

    def set_owner(self, auth_key: str, owner_uid: str):
        if auth_key and owner_uid:
            self.account_owners[str(auth_key)] = str(owner_uid)

    def get_owner(self, auth_uid: str = "", auth_token: str = "") -> Optional[str]:
        if auth_uid and str(auth_uid) in self.account_owners:
            return self.account_owners[str(auth_uid)]
        if auth_token:
            key = f"tok_{str(auth_token)[:20]}"
            if key in self.account_owners:
                return self.account_owners[key]
        return None

    def _infer_owner_from_text(self, message: str) -> Optional[Tuple[str, str]]:
        """
        Scans a log message for a known FreeFire UID or auth-key and returns
        (matched_key, owner_uid) if found. Used to auto-attach owner_uid to
        logs that don't explicitly pass a uid.
        """
        if not message:
            return None
        msg = str(message)
        # 1) Check against registered FreeFire accounts (preferred – longest key first)
        try:
            candidates = sorted(self.accounts.keys(), key=lambda k: len(str(k)), reverse=True)
        except Exception:
            candidates = list(self.accounts.keys())
        for acc_uid in candidates:
            k = str(acc_uid)
            if not k or len(k) < 6:
                continue
            if k in msg:
                owner = self.accounts.get(acc_uid, {}).get("owner_uid")
                if owner:
                    return (k, owner)
        # 2) Check against auth-key registry (uid or tok_xxx)
        try:
            owner_keys = sorted(self.account_owners.keys(), key=lambda k: len(str(k)), reverse=True)
        except Exception:
            owner_keys = list(self.account_owners.keys())
        for ak in owner_keys:
            k = str(ak)
            if not k or len(k) < 6:
                continue
            if k in msg:
                owner = self.account_owners.get(ak)
                if owner:
                    return (k, owner)
        return None

    def log(self, message: str, level: str = "info", uid: Optional[str] = None):
        entry = {
            "time": time.strftime("%H:%M:%S"),
            "level": level,
            "message": message,
            "uid": uid,
            "owner_uid": None,
            "ts": time.time(),
        }

        # ---- 1) Direct uid provided ----
        if uid:
            uid_s = str(uid)
            if uid_s in self.accounts:
                entry["owner_uid"] = self.accounts[uid_s].get("owner_uid")
            elif uid_s in self.account_owners:
                entry["owner_uid"] = self.account_owners[uid_s]

        # ---- 2) Auto-infer owner from message text ----
        if not entry["owner_uid"]:
            inferred = self._infer_owner_from_text(message)
            if inferred:
                matched_key, owner = inferred
                entry["owner_uid"] = owner
                if not entry["uid"]:
                    entry["uid"] = matched_key

        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)

    def register_account(self, uid: str, nickname: str, region: str, level: int, exp: int,
                         likes: int = 0, owner_uid: Optional[str] = None, target_level: int = 0):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            self.accounts[uid_str] = {
                "uid": uid_str,
                "nickname": nickname or f"Player_{uid_str[:6]}",
                "region": region or "BD",
                "level": level or 1,
                "initial_exp": exp,
                "current_exp": exp,
                "gained_exp": 0,
                "likes": likes or 0,
                "status": "ONLINE",
                "matches_played": 0,
                "active_matches": 0,
                "last_match_time": None,
                "last_updated": time.strftime("%H:%M:%S"),
                "owner_uid": owner_uid,
                "target_level": target_level,
            }
        else:
            acc = self.accounts[uid_str]
            if nickname: acc["nickname"] = nickname
            if region: acc["region"] = region
            if level: acc["level"] = level
            if owner_uid: acc["owner_uid"] = owner_uid
            if target_level: acc["target_level"] = target_level
            acc["current_exp"] = exp
            acc["gained_exp"] = max(0, exp - acc["initial_exp"])
            acc["likes"] = likes
            acc["status"] = "ONLINE"
            acc["last_updated"] = time.strftime("%H:%M:%S")
        # Register owner so future logs auto-attach
        if owner_uid:
            self.set_owner(uid_str, owner_uid)
        self.recalc_totals()

    def update_exp(self, uid: str, current_exp: int, level: Optional[int] = None):
        uid_str = str(uid)
        if uid_str in self.accounts:
            acc = self.accounts[uid_str]
            old_exp = acc["current_exp"]
            acc["current_exp"] = current_exp
            if level is not None and level > 0:
                acc["level"] = level
            acc["gained_exp"] = max(0, current_exp - acc["initial_exp"])
            acc["last_updated"] = time.strftime("%H:%M:%S")
            diff = current_exp - old_exp
            if diff > 0:
                self.log(f"Account {acc['nickname']} ({uid_str}) gained +{diff} EXP! Total: +{acc['gained_exp']}", "success", uid_str)
            self.recalc_totals()

    def update_status(self, uid: str, status: str, active_matches: Optional[int] = None):
        uid_str = str(uid)
        if uid_str in self.accounts:
            self.accounts[uid_str]["status"] = status
            if active_matches is not None:
                self.accounts[uid_str]["active_matches"] = active_matches
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match(self, uid: str):
        uid_str = str(uid)
        self.total_matches += 1
        if uid_str in self.accounts:
            self.accounts[uid_str]["matches_played"] += 1
            self.accounts[uid_str]["last_match_time"] = time.strftime("%H:%M:%S")
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")
            self.log(f"Account {self.accounts[uid_str]['nickname']} finished Match #{self.accounts[uid_str]['matches_played']}", "info", uid_str)

    def recalc_totals(self):
        self.total_gained_exp = sum(acc.get("gained_exp", 0) for acc in self.accounts.values())


bot_state = BotState()


# ==================== FIREBASE HELPERS ====================
_token_cache: Dict[str, Dict[str, Any]] = {}
_TOKEN_TTL = 300.0

async def verify_id_token(id_token: str) -> Optional[Dict[str, Any]]:
    if not id_token:
        return None
    now = time.time()
    cached = _token_cache.get(id_token)
    if cached and now < cached["exp"]:
        return cached["data"]
    try:
        url = f"https://identitytoolkit.googleapis.com/v1/accounts:lookup?key={FIREBASE_CONFIG['apiKey']}"
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(url, json={"idToken": id_token})
            if r.status_code != 200:
                return None
            data = r.json()
            users = data.get("users", [])
            if not users:
                return None
            u = users[0]
            result = {"uid": u["localId"], "email": u.get("email", "").lower()}
            _token_cache[id_token] = {"exp": now + _TOKEN_TTL, "data": result}
            return result
    except Exception:
        return None


async def fb_set(path: str, data: Any, id_token: str) -> bool:
    try:
        url = f"{FIREBASE_CONFIG['databaseURL']}/{path}.json?auth={id_token}"
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.put(url, json=data)
            return r.status_code == 200
    except Exception:
        return False


async def fb_patch(path: str, data: Any, id_token: str) -> bool:
    try:
        url = f"{FIREBASE_CONFIG['databaseURL']}/{path}.json?auth={id_token}"
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.patch(url, json=data)
            return r.status_code == 200
    except Exception:
        return False


async def fb_delete(path: str, id_token: str) -> bool:
    try:
        url = f"{FIREBASE_CONFIG['databaseURL']}/{path}.json?auth={id_token}"
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.delete(url)
            return r.status_code == 200
    except Exception:
        return False


# ==================== AUTH MIDDLEWARE ====================
@web.middleware
async def auth_mw(request: web.Request, handler):
    if request.path in ("/", "/admin") or request.path.startswith("/static"):
        return await handler(request)
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return web.json_response({"status": "error", "error": "Unauthorized"}, status=401)
    token = auth[7:].strip()
    user = await verify_id_token(token)
    if not user:
        return web.json_response({"status": "error", "error": "Invalid/Expired Token"}, status=401)
    request["user"] = user
    request["id_token"] = token
    return await handler(request)


def is_admin(user: Dict[str, Any]) -> bool:
    return user.get("email", "").lower() in ADMIN_EMAILS


# ==================== HTTP HANDLERS ====================
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

async def handle_index(request: web.Request) -> web.Response:
    path = os.path.join(TEMPLATE_DIR, "index.html")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html", charset="utf-8")
    return web.Response(text="<h1>templates/index.html missing</h1>", content_type="text/html")


async def handle_admin(request: web.Request) -> web.Response:
    path = os.path.join(TEMPLATE_DIR, "admin.html")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html", charset="utf-8")
    return web.Response(text="<h1>templates/admin.html missing</h1>", content_type="text/html")


def _filter_accounts_for_user(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    uid = user["uid"]
    return [a for a in bot_state.accounts.values() if a.get("owner_uid") == uid]


def _filter_logs_for_user(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return logs visible to this user.
    A log is visible if:
      - its owner_uid matches user.uid, OR
      - the log has NO owner_uid AND the message references any of
        the user's own FreeFire account UIDs (fallback).
    """
    uid = user["uid"]
    own_uids = {str(a.get("uid")) for a in bot_state.accounts.values()
                if a.get("owner_uid") == uid and a.get("uid")}
    own_uids.discard("")
    own_uids.discard("None")

    out: List[Dict[str, Any]] = []
    for l in bot_state.logs:
        if l.get("owner_uid") == uid:
            out.append(l)
            continue
        if not l.get("owner_uid") and own_uids:
            msg = str(l.get("message", ""))
            for u in own_uids:
                if len(u) >= 6 and u in msg:
                    out.append(l)
                    break
    return out


async def handle_get_stats(request: web.Request) -> web.Response:
    user = request["user"]
    if is_admin(user):
        accounts_data = list(bot_state.accounts.values())
        logs = bot_state.logs[-120:]
    else:
        accounts_data = _filter_accounts_for_user(user)
        logs = _filter_logs_for_user(user)[-80:]

    accounts_data.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)
    return web.json_response({
        "total_accounts": len(accounts_data),
        "total_matches": sum(a.get("matches_played", 0) for a in accounts_data),
        "total_gained_exp": sum(a.get("gained_exp", 0) for a in accounts_data),
        "accounts": accounts_data,
        "logs": logs,
        "uptime": int(time.time() - bot_state.start_time),
        "is_admin": is_admin(user),
        "user": {"uid": user["uid"], "email": user.get("email", "")}
    })


async def handle_add_account(request: web.Request) -> web.Response:
    try:
        user = request["user"]
        id_token = request["id_token"]
        data = await request.json()
        region = str(data.get("region", "BD")).strip().upper() or "BD"
        target_level = int(data.get("target_level", 0) or 0)

        accounts_file = "accounts.json"
        existing = []
        if os.path.exists(accounts_file):
            try:
                with open(accounts_file, "r", encoding="utf-8") as f:
                    existing = json.load(f) or []
            except Exception:
                existing = []

        if "uid" in data and "password" in data:
            uid = str(data["uid"]).strip()
            pwd = str(data["password"]).strip()
            if not uid or not pwd:
                return web.json_response({"status": "error", "error": "UID & Password required"})
            existing = [a for a in existing if str(a.get("uid")) != uid]
            entry = {"uid": uid, "password": pwd, "region": region,
                     "target_level": target_level, "owner_uid": user["uid"]}
            existing.append(entry)
            auth_key = uid
            payload = {"uid": uid, "password": pwd, "region": region, "target_level": target_level}
        elif "token" in data:
            token = str(data["token"]).strip()
            if not token:
                return web.json_response({"status": "error", "error": "Token required"})
            existing = [a for a in existing if a.get("token") != token]
            entry = {"token": token, "region": region,
                     "target_level": target_level, "owner_uid": user["uid"]}
            existing.append(entry)
            auth_key = f"tok_{token[:20]}"
            payload = {"token": token, "region": region, "target_level": target_level}
        else:
            return web.json_response({"status": "error", "error": "Invalid payload"})

        with open(accounts_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)

        bot_state.set_owner(auth_key, user["uid"])
        safe_key = auth_key.replace(".", "_").replace("@", "_").replace("$", "_").replace("#", "_").replace("[", "_").replace("]", "_").replace("/", "_")
        entry_meta = dict(payload)
        entry_meta["auth_key"] = auth_key
        entry_meta["owner_uid"] = user["uid"]
        entry_meta["added_at"] = int(time.time())
        await fb_set(f"user_accounts/{user['uid']}/{safe_key}", entry_meta, id_token)

        bot_state.log(f"New account added: {data.get('uid') or 'Token'}", "success", str(auth_key))

        if "on_account_added" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_account_added"]({**data, "owner_uid": user["uid"], "target_level": target_level, "region": region}))

        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_delete_account(request: web.Request) -> web.Response:
    try:
        user = request["user"]
        id_token = request["id_token"]
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        auth_key = data.get("auth_key", "")

        accounts_file = "accounts.json"
        existing = []
        if os.path.exists(accounts_file):
            with open(accounts_file, "r", encoding="utf-8") as f:
                existing = json.load(f) or []

        def is_owned(entry):
            if is_admin(user):
                return True
            return entry.get("owner_uid") == user["uid"]

        new_list = []
        for a in existing:
            aid = str(a.get("uid") or a.get("token", "")[:20])
            if aid == uid or a.get("token") == data.get("token"):
                if not is_owned(a):
                    continue
                ak = a.get("uid") or f"tok_{a.get('token','')[:20]}"
                bot_state.account_owners.pop(str(ak), None)
                if a.get("owner_uid"):
                    safe_key = str(ak).replace(".", "_").replace("@", "_").replace("$", "_").replace("#", "_").replace("[", "_").replace("]", "_").replace("/", "_")
                    await fb_delete(f"user_accounts/{a['owner_uid']}/{safe_key}", id_token)
            else:
                new_list.append(a)

        with open(accounts_file, "w", encoding="utf-8") as f:
            json.dump(new_list, f, indent=2)

        if uid in bot_state.accounts:
            if is_admin(user) or bot_state.accounts[uid].get("owner_uid") == user["uid"]:
                del bot_state.accounts[uid]

        if uid in bot_state.account_workers:
            bot_state.account_workers[uid].cancel()
            del bot_state.account_workers[uid]

        bot_state.log(f"Account {uid} removed.", "warning", uid)
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_refresh_account(request: web.Request) -> web.Response:
    try:
        user = request["user"]
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if not is_admin(user):
            acc = bot_state.accounts.get(uid)
            if acc and acc.get("owner_uid") != user["uid"]:
                return web.json_response({"status": "error", "error": "Forbidden"}, status=403)
        if "on_refresh_account" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_refresh_account"](uid))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_toggle_account(request: web.Request) -> web.Response:
    try:
        user = request["user"]
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        action = data.get("action", "")
        if not is_admin(user):
            acc = bot_state.accounts.get(uid)
            if acc and acc.get("owner_uid") != user["uid"]:
                return web.json_response({"status": "error", "error": "Forbidden"}, status=403)
        if "on_account_toggled" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_account_toggled"]({"uid": uid, "action": action}))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_toggle_all(request: web.Request) -> web.Response:
    try:
        user = request["user"]
        data = await request.json()
        action = data.get("action", "pause")
        if "on_toggle_all" in bot_state.refresh_callbacks:
            asyncio.create_task(bot_state.refresh_callbacks["on_toggle_all"]({"action": action, "owner_uid": user["uid"], "is_admin": is_admin(user)}))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


# ==================== ADMIN ENDPOINTS ====================
async def handle_admin_whoami(request: web.Request) -> web.Response:
    user = request["user"]
    return web.json_response({
        "is_admin": is_admin(user),
        "email": user.get("email", ""),
        "uid": user["uid"]
    })


async def handle_admin_overview(request: web.Request) -> web.Response:
    user = request["user"]
    if not is_admin(user):
        return web.json_response({"status": "error", "error": "Admin only"}, status=403)

    owners: Dict[str, Dict[str, Any]] = {}
    for a in bot_state.accounts.values():
        owner = a.get("owner_uid") or "unknown"
        if owner not in owners:
            owners[owner] = {"owner_uid": owner, "accounts": [], "total_matches": 0, "total_exp": 0}
        owners[owner]["accounts"].append(a)
        owners[owner]["total_matches"] += a.get("matches_played", 0)
        owners[owner]["total_exp"] += a.get("gained_exp", 0)

    return web.json_response({
        "status": "ok",
        "total_users": len(owners),
        "total_accounts": len(bot_state.accounts),
        "total_matches": bot_state.total_matches,
        "total_gained_exp": bot_state.total_gained_exp,
        "total_active_workers": len(bot_state.account_workers),
        "owners": list(owners.values()),
        "recent_logs": bot_state.logs[-200:],
        "uptime": int(time.time() - bot_state.start_time),
    })


# ==================== SERVER STARTUP ====================
async def start_web_dashboard(host: str = "0.0.0.0", port: int = 5000):
    app = web.Application(middlewares=[auth_mw])
    app.router.add_get("/", handle_index)
    app.router.add_get("/admin", handle_admin)
    app.router.add_get("/api/stats", handle_get_stats)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/account/refresh", handle_refresh_account)
    app.router.add_post("/api/account/toggle", handle_toggle_account)
    app.router.add_post("/api/account/toggle_all", handle_toggle_all)
    app.router.add_get("/api/admin/whoami", handle_admin_whoami)
    app.router.add_get("/api/admin/overview", handle_admin_overview)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"\033[92m[+] Web Dashboard: http://localhost:{port}\033[0m")
    print(f"\033[92m[+] Admin Panel  : http://localhost:{port}/admin\033[0m")
