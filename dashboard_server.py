# dashboard_server.py
"""
Per-user isolated dashboard.
Railway pulls active users & their bots from the PHP Web Hosting API.
Each bot worker is tagged with (user_id, bot_id) so logs/stats go to right owner.
"""
import asyncio
import json
import os
import time
from typing import Dict, List, Any, Optional
from aiohttp import web

import web_api


class BotState:
    def __init__(self):
        # accounts keyed by "u{user_id}_b{bot_id}"
        self.accounts: Dict[str, Dict[str, Any]] = {}
        # logs keyed by the same composite key
        self.logs: Dict[str, List[Dict[str, Any]]] = {}
        self.max_logs = 200
        self.start_time = time.time()
        self.account_workers: Dict[str, asyncio.Task] = {}
        self.account_meta: Dict[str, Dict[str, Any]] = {}  # key → {user_id, bot_id, username}

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
            # Push to web hosting (fire & forget)
            asyncio.create_task(web_api.report_log(user_id, bot_id, level, message))
        else:
            # system-level log — keep in global buffer
            self.logs.setdefault("_system", []).append(entry)
            if len(self.logs["_system"]) > self.max_logs:
                self.logs["_system"].pop(0)
        # also echo to stdout
        try:
            print(f"[{level.upper()}] {message}")
        except Exception:
            pass

    def register_account(self, user_id: int, bot_id: int, uid: str, nickname: str,
                         region: str, level: int, exp: int, likes: int = 0):
        k = self._key(user_id, bot_id)
        self.account_meta[k] = {"user_id": user_id, "bot_id": bot_id}
        if k not in self.accounts:
            self.accounts[k] = {
                "user_id": user_id, "bot_id": bot_id,
                "uid": uid, "nickname": nickname or f"Player_{uid[:6]}",
                "region": region or "BD", "level": level or 1,
                "initial_exp": exp, "current_exp": exp, "gained_exp": 0,
                "likes": likes or 0, "status": "ONLINE",
                "matches_played": 0, "active_matches": 0,
                "last_match_time": None,
                "last_updated": time.strftime("%H:%M:%S"),
            }
        else:
            a = self.accounts[k]
            if nickname: a["nickname"] = nickname
            if region: a["region"] = region
            if level: a["level"] = level
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
        if not a: return
        await web_api.report_stats(bot_id, {
            "nickname": a["nickname"], "level": a["level"],
            "initial_exp": a["initial_exp"], "current_exp": a["current_exp"],
            "gained_exp": a["gained_exp"], "likes": a["likes"],
            "status": a["status"], "matches_played": a["matches_played"],
            "active_matches": a["active_matches"],
            "last_match_time": a["last_match_time"],
        })


bot_state = BotState()


# ============== PERIODIC SYNC WITH WEB HOSTING ==============

_sync_lock = asyncio.Lock()


async def _sync_loop():
    """Every 20s: pull users from web API, add workers for new bots, stop workers for deleted ones."""
    from Main import account_loop_guest, account_loop_token  # lazy import to avoid circular

    while True:
        try:
            async with _sync_lock:
                users = await web_api.pull_users()
                active_keys = set()
                for u in users:
                    user_id = u["user_id"]
                    username = u.get("username", f"user{user_id}")
                    for b in u["bots"]:
                        bot_id = b["id"]
                        k = bot_state._key(user_id, bot_id)
                        active_keys.add(k)

                        if k in bot_state.account_workers and not bot_state.account_workers[k].done():
                            continue

                        # start new worker
                        bot_state.account_meta[k] = {"user_id": user_id, "bot_id": bot_id,
                                                      "username": username}
                        if b.get("access_token"):
                            task = asyncio.create_task(
                                account_loop_token(b["access_token"], user_id, bot_id,
                                                   region=b.get("region", "BD"))
                            )
                        elif b.get("uid") and b.get("password"):
                            task = asyncio.create_task(
                                account_loop_guest(b["uid"], b["password"], user_id, bot_id,
                                                   region=b.get("region", "BD"))
                            )
                        else:
                            continue
                        bot_state.account_workers[k] = task
                        bot_state.log(f"🚀 Worker started for {username} bot #{bot_id}",
                                      "success", user_id=user_id, bot_id=bot_id)

                # stop deleted/paused bots
                for k in list(bot_state.account_workers.keys()):
                    if k not in active_keys:
                        task = bot_state.account_workers[k]
                        if not task.done(): task.cancel()
                        del bot_state.account_workers[k]
                        if k in bot_state.accounts:
                            del bot_state.accounts[k]
                        bot_state.log(f"🛑 Worker stopped (removed/paused).", "warning",
                                      user_id=bot_state.account_meta.get(k, {}).get("user_id"),
                                      bot_id=bot_state.account_meta.get(k, {}).get("bot_id"))
                        bot_state.account_meta.pop(k, None)

                # push stats to web hosting every cycle
                for k, a in list(bot_state.accounts.items()):
                    await bot_state.push_stats(a["user_id"], a["bot_id"])

        except Exception as e:
            print(f"[SYNC] error: {e}")
        await asyncio.sleep(20)


# ============== OPTIONAL: local mini web UI (for debugging) ==============

async def handle_stats(request: web.Request) -> web.Response:
    """Public JSON snapshot (nobody should rely on this; main dashboard is on PHP)."""
    return web.json_response({
        "total_workers": len(bot_state.account_workers),
        "accounts": list(bot_state.accounts.values()),
        "uptime": int(time.time() - bot_state.start_time),
    })


async def start_web_dashboard(host: str = "0.0.0.0", port: int = 5000):
    app = web.Application()
    app.router.add_get("/api/stats", handle_stats)
    app.router.add_get("/health", lambda r: web.Response(text="OK"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"\033[92m[+] Debug endpoint http://localhost:{port}/api/stats\033[0m")

    # Kick off sync loop
    asyncio.create_task(_sync_loop())