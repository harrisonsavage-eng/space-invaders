"""
Space Invaders Multiplayer Server
Single worker, in-memory rooms, aiohttp WebSocket
"""
import os, json, random, string, time
from aiohttp import web, WSMsgType

PORT = int(os.environ.get("PORT", 10000))

rooms = {}    # code -> {players:{pid:ws}, mode, max_players}
ws_info = {}  # ws -> {code, pid}

def make_code():
    while True:
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        if code not in rooms:
            return code

async def tx(ws, msg):
    try:
        await ws.send_str(json.dumps(msg))
    except Exception as e:
        print(f"[tx error] {e}")

async def broadcast(code, msg, exclude_pid=None):
    room = rooms.get(code)
    if not room:
        return
    data = json.dumps(msg)
    for pid, ws in list(room["players"].items()):
        if pid == exclude_pid:
            continue
        try:
            await ws.send_str(data)
        except Exception as e:
            print(f"[broadcast error pid={pid}] {e}")

async def cleanup(ws):
    info = ws_info.pop(ws, None)
    if not info:
        return
    code, pid = info["code"], info["pid"]
    room = rooms.get(code)
    if not room:
        return
    room["players"].pop(pid, None)
    print(f"[Server] P{pid} left room {code}. Players left: {len(room['players'])}")
    if room["players"]:
        await broadcast(code, {"type": "player_left", "player_id": pid})
    else:
        del rooms[code]
        print(f"[Server] Room {code} deleted")

async def index(request):
    return web.FileResponse("index.html")

async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)
    print(f"[Server] Connection opened. Active rooms: {list(rooms.keys())}")

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    d = json.loads(msg.data)
                except Exception:
                    continue

                t = d.get("type", "")
                print(f"[Server] MSG={t} rooms={list(rooms.keys())}")

                if t == "ping":
                    await tx(ws, {"type": "pong"})

                elif t == "create_room":
                    code = make_code()
                    mode = d.get("mode", "coop")
                    max_p = max(2, min(4, int(d.get("max_players", 2))))
                    rooms[code] = {
                        "players": {1: ws},
                        "mode": mode,
                        "max_players": max_p
                    }
                    ws_info[ws] = {"code": code, "pid": 1}
                    print(f"[Server] Room {code} CREATED mode={mode} max={max_p}")
                    await tx(ws, {
                        "type": "room_created",
                        "room_code": code,
                        "player_id": 1,
                        "mode": mode,
                        "max_players": max_p
                    })

                elif t == "join_room":
                    code = str(d.get("room_code", "")).upper().strip()
                    print(f"[Server] JOIN attempt code='{code}' available={list(rooms.keys())}")
                    room = rooms.get(code)
                    if not room:
                        print(f"[Server] Room '{code}' NOT FOUND")
                        await tx(ws, {"type": "error", "msg": f"Room {code} not found. Check the code!"})
                    elif len(room["players"]) >= room["max_players"]:
                        await tx(ws, {"type": "error", "msg": "Room is full!"})
                    else:
                        pid = len(room["players"]) + 1
                        room["players"][pid] = ws
                        ws_info[ws] = {"code": code, "pid": pid}
                        count = len(room["players"])
                        print(f"[Server] P{pid} joined room {code} ({count}/{room['max_players']})")
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
                            print(f"[Server] Room {code} FULL — starting!")
                            await broadcast(code, {
                                "type": "start_game",
                                "mode": room["mode"],
                                "player_count": count
                            })

                elif t == "chat":
                    info = ws_info.get(ws)
                    if info:
                        await broadcast(info["code"], {
                            "type": "chat",
                            "player_id": info["pid"],
                            "text": str(d.get("text", ""))[:80]
                        })

                else:
                    info = ws_info.get(ws)
                    if info:
                        d["from_player"] = info["pid"]
                        d["player_id"] = info["pid"]
                        await broadcast(info["code"], d, exclude_pid=info["pid"])

            elif msg.type == WSMsgType.ERROR:
                print(f"[Server] WS error: {ws.exception()}")

    except Exception as e:
        print(f"[Server] Handler error: {e}")
    finally:
        await cleanup(ws)
        print(f"[Server] Connection closed. Rooms: {list(rooms.keys())}")

    return ws

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/index.html", index)
app.router.add_get("/ws", ws_handler)

if __name__ == "__main__":
    print(f"[Server] Starting on port {PORT}")
    # IMPORTANT: workers=1 keeps all rooms in same memory space
    web.run_app(app, host="0.0.0.0", port=PORT, access_log=None)
