# web_api.py
"""
Bridges Railway bot to Web Hosting PHP API.
"""
import os
import httpx
from typing import Dict, List, Any, Optional

WEB_BASE_URL = os.environ.get("WEB_BASE_URL", "https://level.buildspare.site")
WEB_API_KEY  = os.environ.get("WEB_API_KEY", "uB7xN2mVfK9pLqT4sW8yDcH5jR3aZ6eG")

_client = httpx.AsyncClient(timeout=15.0, verify=False)
_HEADERS = {"X-API-Key": WEB_API_KEY}


async def pull_users() -> List[Dict[str, Any]]:
    """Returns: [{user_id, username, bots:[{id,uid,password,access_token,region}]}]"""
    try:
        r = await _client.get(f"{WEB_BASE_URL}/api/railway.php",
                              params={"action": "users"}, headers=_HEADERS)
        r.raise_for_status()
        data = r.json()
        n = len(data.get("users", []))
        print(f"[WEB-API] ✅ pull_users OK → {n} users")
        return data.get("users", [])
    except Exception as e:
        print(f"[WEB-API] ❌ pull_users error: {type(e).__name__}: {e}")
        return []


async def report_log(user_id: int, bot_id: Optional[int], level: str, message: str):
    try:
        r = await _client.post(f"{WEB_BASE_URL}/api/railway.php",
                               params={"action": "report_log"}, headers=_HEADERS,
                               json={"user_id": user_id, "bot_id": bot_id,
                                     "level": level, "message": message[:900]})
        if r.status_code != 200:
            print(f"[WEB-API] ❌ report_log HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[WEB-API] ❌ report_log error: {type(e).__name__}: {e}")


async def report_stats(bot_id: int, stats: Dict[str, Any]):
    try:
        r = await _client.post(f"{WEB_BASE_URL}/api/railway.php",
                               params={"action": "report_stats"}, headers=_HEADERS,
                               json={"bot_id": bot_id, **stats})
        if r.status_code != 200:
            print(f"[WEB-API] ❌ report_stats HTTP {r.status_code}: {r.text[:200]}")
        else:
            resp = r.json()
            if resp.get("error"):
                print(f"[WEB-API] ❌ report_stats error: {resp}")
            else:
                print(f"[WEB-API] ✅ report_stats bot={bot_id} rows={resp.get('rows', '?')}")
    except Exception as e:
        print(f"[WEB-API] ❌ report_stats error: {type(e).__name__}: {e}")
