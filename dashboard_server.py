# dashboard_server.py
"""
Per-user isolated dashboard + stats pusher for Railway.
"""
import asyncio
import time
import traceback
from typing import Dict, List, Any, Optional
from aiohttp import web

import web_api


class BotState:
    def __init__(self):
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: Dict[str, List[Dict[str, Any]]] = {}
        self.max_logs = 200
        self.start_time = time.time()
        self.account_workers: Dict[str, asyncio.Task] = {}
        self.account_meta: Dict[str, Dict[str, Any]] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}

    def _key(self, user_id: int, bot_id: int) -> str:
        return f"u{user_id}_b{bot_id}"

    def log(self, message: str, level: str = "info",
            user_id: Optional[int] = None, bot_id: Optional[int] = None,
            uid: Optional[str] = None):
        entry = {"time": time.strftime("%H:%M:%S"), "level": level,
                 "message": message, "uid": uid}
        if user_id and bot_id:
            k = self._key(user_id, bot_id)
            self.logs.setdefault(k, []).append(entry)
            if len(self.logs[k]) > self.max_logs:
                self.logs[k].pop(0)
            asyncio.create_task(web_api.report_log(user_id, bot_id, level, message))
        else:
            self.logs.setdefault("_system", []).append(entry)
            if len(self.logs["_system"]) > self.max_logs:
                self.logs["_system"].pop(0)
        try:
            print(f"[{level.upper()}] {message}")
        except Exception:
            pass

    def register_account(self, user_id: int, bot_id: int, uid: str, nickname: str,
                         region: str, level: int, exp: int, likes: int = 0,
                         login_uid: str = ""):
        k = self._key(user_id, bot_id)
        self.account_meta[k] = {"user_id": user_id, "bot_id": bot_id}
        if k not in self.accounts:
            self.accounts[k] = {
                "user_id": user_id, "bot_id": bot_id,
                "uid": uid, "login_uid": login_uid,
                "nickname": nickname or f"Player_{uid[:6]}",
                "region": region or "BD", "level": level or 1,
                "initial_exp": exp, "current_exp": exp, "gained_exp": 0,
                "likes": likes or 0, "status": "ONLINE",
                "matches_played": 0, "active_matches": 0,
                "last_match_time": None,
                "last_updated": time.strftime("%H:%M:%S"),
            }
            print(f"[REGISTER] NEW user={user_id} bot={bot_id} server_uid={uid} login_uid={login_uid} nickname={nickname}")
        else:
            a = self.accounts[k]
            if nickname: a["nickname"] = nickname
            if region: a["region"] = region
            if level: a["level"] = level
            if login_uid: a["login_uid"] = login_uid
            a["current_exp"] = exp
            a["gained_exp"] = max(0, exp - a["initial_exp"])
            a["likes"] = likes
            a["status"] = "ONLINE"
            a["last_updated"] = time.strftime("%H:%M:%S")

    def update_exp(self, user_id: int, bot_id: int, current_exp: int, level: Optional[int] = None):
        k = self._key(user_id, bot_id)
        if k not in self.accounts: return
        a = self.accounts[k]
        a["current_exp"] = current_exp
        if level and level > 0: a["level"] = level
        a["gained_exp"] = max(0, current_exp - a["initial_exp"])
        a["last_updated"] = time.strftime("%H:%M:%S")

    def update_status(self, user_id: int, bot_id: int, status: str, active_matches: Optional[int] = None):
        k = self._key(user_id, bot_id)
        if k not in self.accounts: return
        a = self.accounts[k]
        a["status"] = status
        if active_matches is not None: a["active_matches"] = active_matches
        a["last_updated"] = time.strftime("%H:%M:%S")

    def increment_match(self, user_id: int, bot_id: int):
        k = self._key(user_id, bot_id)
        if k not in self.accounts: return
        a = self.accounts[k]
        a["matches_played"] += 1
        a["last_match_time"] = time.strftime("%H:%M:%S")
        a["last_updated"] = time.strftime("%H:%M:%S")

    async def push_stats(self, user_id: int, bot_id: int):
        k = self._key(user_id, bot_id)
        a = self.accounts.get(k)
        if not a:
            print(f"[PUSH-STATS] ❌ No account user={user_id} bot={bot_id}")
            return
        payload = {
            "nickname":        a["nickname"],
            "account_id":      a.get("uid"),          # server UID from MajorLogin
            "login_uid":       a.get("login_uid"),    # guest UID user entered
            "level":           a["level"],
            "initial_exp":     a["initial_exp"],
            "current_exp":     a["current_exp"],
            "gained_exp":      a["gained_exp"],
            "likes":           a["likes"],
            "status":          a["status"],
            "matches_played":  a["matches_played"],
            "active_matches":  a["active_matches"],
            "last_match_time": a["last_match_time"],
        }
        print(f"[PUSH-STATS] user={user_id} bot={bot_id} server_uid={a.get('uid')} "
              f"login_uid={a.get('login_uid')} status={a['status']} "
              f"gained={a['gained_exp']} matches={a['matches_played']}")
        await web_api.report_stats(bot_id, payload)


bot_state = BotState()


# ==================== SYNC LOOP ====================
_sync_lock = asyncio.Lock()


async def _sync_loop():
    from Main import account_loop_guest, account_loop_token
    await asyncio.sleep(3)

    while True:
        cycle_start = time.time()
        try:
            async with _sync_lock:
                print(f"\n[SYNC] ══ Cycle start ══")
                users = await web_api.pull_users()
                print(f"[SYNC] Pulled {len(users)} users")

                active_keys = set()

                # STEP 1: Start workers
                for u in users:
                    user_id = int(u["user_id"])
                    username = u.get("username", f"user{user_id}")
                    bots = u.get("bots", [])
                    print(f"[SYNC] User {user_id} ({username}) → {len(bots)} bots")

                    for b in bots:
                        bot_id = int(b["id"])
                        k = bot_state._key(user_id, bot_id)
                        active_keys.add(k)

                        existing = bot_state.account_workers.get(k)
                        if existing and not existing.done():
                            continue  # already running

                        bot_state.account_meta[k] = {
                            "user_id": user_id, "bot_id": bot_id, "username": username,
                        }

                        if b.get("access_token"):
                            print(f"[SYNC] 🚀 TOKEN worker for {username} bot #{bot_id}")
                            task = asyncio.create_task(account_loop_token(
                                b["access_token"], user_id, bot_id,
                                region=b.get("region", "BD")
                            ))
                            bot_state.account_workers[k] = task
                        elif b.get("uid") and b.get("password"):
                            print(f"[SYNC] 🚀 GUEST worker for {username} bot #{bot_id} (UID: {b['uid']})")
                            task = asyncio.create_task(account_loop_guest(
                                b["uid"], b["password"], user_id, bot_id,
                                region=b.get("region", "BD")
                            ))
                            bot_state.account_workers[k] = task
                        else:
                            print(f"[SYNC] ⚠️ Bot {bot_id} no credentials")

                # STEP 2: Stop removed workers
                for k in list(bot_state.account_workers.keys()):
                    if k not in active_keys:
                        task = bot_state.account_workers[k]
                        if not task.done():
                            task.cancel()
                        del bot_state.account_workers[k]
                        meta = bot_state.account_meta.get(k, {})
                        print(f"[SYNC] 🛑 Stopped worker user={meta.get('user_id')} bot={meta.get('bot_id')}")
                        bot_state.account_meta.pop(k, None)

                # STEP 3: Cleanup stale accounts
                for k in list(bot_state.accounts.keys()):
                    if k not in active_keys:
                        meta = bot_state.account_meta.get(k) or bot_state.accounts[k]
                        print(f"[SYNC] 🧹 Cleanup user={meta.get('user_id')} bot={meta.get('bot_id')}")
                        del bot_state.accounts[k]

                # STEP 4: Push stats
                if bot_state.accounts:
                    print(f"[SYNC] Pushing stats for {len(bot_state.accounts)} account(s)")
                    for k, a in list(bot_state.accounts.items()):
                        try:
                            await bot_state.push_stats(a["user_id"], a["bot_id"])
                        except Exception as e:
                            print(f"[SYNC] ❌ push_stats {k}: {e}")
                else:
                    print(f"[SYNC] No active accounts to push")

                print(f"[SYNC] ══ Cycle done in {time.time()-cycle_start:.2f}s ══\n")

        except Exception as e:
            print(f"[SYNC] ❌ Loop error: {type(e).__name__}: {e}")
            traceback.print_exc()

        await asyncio.sleep(20)


# ==================== DEBUG HTTP SERVER ====================
async def handle_root(request):
    return web.Response(text="7H SIAM Bot Engine Running")

async def handle_health(request):
    return web.Response(text="OK")

async def handle_stats(request):
    return web.json_response({
        "total_workers":  len(bot_state.account_workers),
        "total_accounts": len(bot_state.accounts),
        "accounts":       list(bot_state.accounts.values()),
        "uptime":         int(time.time() - bot_state.start_time),
    })


async def start_web_dashboard(host: str = "0.0.0.0", port: int = 5000):
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/api/stats", handle_stats)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    print(f"\033[92m[+] Debug: http://localhost:{port}/api/stats\033[0m")
    print(f"\033[92m[+] Health: http://localhost:{port}/health\033[0m")

    asyncio.create_task(_sync_loop())
    print(f"\033[92m[+] Sync loop started (20s interval)\033[0m")
