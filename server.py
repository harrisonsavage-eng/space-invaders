"""
Space Invaders Server - Rock solid room system
"""
import os, json, random, string, time, asyncio
from aiohttp import web, WSMsgType

PORT = int(os.environ.get("PORT", 10000))

# rooms[code] = {"players": {pid: ws}, "mode": str, "max_players": int}
rooms = {}
# ws_rooms[ws] = {"code": str, "pid": int}
ws_rooms = {}

def make_code():
    while True:
        code = ''.join(random.choices(string.ascii_uppercase, k=5))
        if code not in rooms:
            return code

async def broadcast(code, msg, exclude_pid=None):
    room = rooms.get(code)
    if not room: return
    data = json.dumps(msg)
    for pid, ws in list(room["players"].items()):
        if pid == exclude_pid: continue
        try: await ws.send_str(data)
        except: pass

async def tx(ws, msg):
    try: await ws.send_str(json.dumps(msg))
    except: pass

async def remove_ws(ws):
    info = ws_rooms.pop(ws, None)
    if not info: return
    code, pid = info["code"], info["pid"]
    room = rooms.get(code)
    if not room: return
    room["players"].pop(pid, None)
    await broadcast(code, {"type": "player_left", "player_id": pid})
    if not room["players"]:
        rooms.pop(code, None)
        print(f"[Server] Room {code} closed")

async def index(request):
    return web.FileResponse("index.html")

async def wshandler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    print(f"[Server] New connection")
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT: continue
            try: data = json.loads(msg.data)
            except: continue
            t = data.get("type", "")
            print(f"[Server] Got: {t} | rooms: {list(rooms.keys())}")

            if t == "create_room":
                code = make_code()
                mode = data.get("mode", "coop")
                max_p = int(data.get("max_players", 2))
                rooms[code] = {"players": {1: ws}, "mode": mode, "max_players": max_p}
                ws_rooms[ws] = {"code": code, "pid": 1}
                print(f"[Server] Created room {code} mode={mode} max={max_p}")
                await tx(ws, {
                    "type": "room_created",
                    "room_code": code,
                    "player_id": 1,
                    "mode": mode,
                    "max_players": max_p
                })

            elif t == "join_room":
                code = data.get("room_code", "").upper().strip()
                print(f"[Server] Join attempt: '{code}' | available: {list(rooms.keys())}")
                room = rooms.get(code)
                if not room:
                    await tx(ws, {"type": "error", "msg": f"Room '{code}' not found"})
                elif len(room["players"]) >= room["max_players"]:
                    await tx(ws, {"type": "error", "msg": "Room is full"})
                else:
                    pid = len(room["players"]) + 1
                    room["players"][pid] = ws
                    ws_rooms[ws] = {"code": code, "pid": pid}
                    count = len(room["players"])
                    print(f"[Server] Player {pid} joined room {code} ({count}/{room['max_players']})")
                    await tx(ws, {
                        "type": "room_joined",
                        "room_code": code,
                        "player_id": pid,
                        "mode": room["mode"],
                        "max_players": room["max_players"]
                    })
                    await broadcast(code, {
                        "type": "player_joined",
                        "player_id": pid,
                        "current_count": count,
                        "max_players": room["max_players"]
                    }, exclude_pid=pid)
                    if count >= room["max_players"]:
                        print(f"[Server] Room {code} full — starting game!")
                        await broadcast(code, {
                            "type": "start_game",
                            "mode": room["mode"],
                            "player_count": count
                        })

            elif t == "chat":
                info = ws_rooms.get(ws)
                if info:
                    pid = info["pid"]
                    await broadcast(info["code"], {
                        "type": "chat",
                        "player_id": pid,
                        "text": str(data.get("text", ""))[:80],
                        "ts": time.time()
                    })

            else:
                info = ws_rooms.get(ws)
                if info:
                    data["from_player"] = info["pid"]
                    data["player_id"] = info["pid"]
                    await broadcast(info["code"], data, exclude_pid=info["pid"])

    except Exception as e:
        print(f"[Server] Error: {e}")
    finally:
        await remove_ws(ws)
        print(f"[Server] Connection closed")
    return ws

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/index.html", index)
app.router.add_get("/ws", wshandler)

if __name__ == "__main__":
    print(f"[Server] Starting on port {PORT}")
    web.run_app(app, host="0.0.0.0", port=PORT, access_log=None)
