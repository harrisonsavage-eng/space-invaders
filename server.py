"""
server.py - Space Invaders Co-op
Uses aiohttp - handles HTTP and WebSocket on the same port perfectly.
Works on Render free tier.
"""

from aiohttp import web
import json
import random
import time
import os
import string

PORT = int(os.environ.get("PORT", 10000))

rooms = {}
ws_to_room = {}

def make_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))

async def broadcast(code, msg, exclude=None):
    room = rooms.get(code)
    if not room: return
    data = json.dumps(msg)
    for ws in list(room["sockets"]):
        if ws is exclude: continue
        try: await ws.send_str(data)
        except: pass

async def tx(ws, msg):
    try: await ws.send_str(json.dumps(msg))
    except: pass

async def drop(ws):
    code = ws_to_room.pop(id(ws), None)
    if not code: return
    room = rooms.get(code)
    if room:
        pid = room["players"].pop(id(ws), None)
        room["sockets"].discard(ws)
        if pid: await broadcast(code, {"type":"player_left","player_id":pid})
        if not room["sockets"]: rooms.pop(code, None)

async def index(request):
    """Serve the game HTML."""
    return web.FileResponse("index.html")

async def websocket_handler(request):
    """Handle WebSocket game connections."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    try:
        async for raw in ws:
            if raw.type != web.WSMsgType.TEXT:
                continue
            try: msg = json.loads(raw.data)
            except: continue
            t = msg.get("type")

            if t == "create_room":
                code = make_code()
                rooms[code] = {"sockets":{ws},"players":{id(ws):1}}
                ws_to_room[id(ws)] = code
                await tx(ws, {"type":"room_created","room_code":code,"player_id":1})
                print(f"[WS] Room {code} created")

            elif t == "join_room":
                code = msg.get("room_code","").upper().strip()
                room = rooms.get(code)
                if not room:
                    await tx(ws, {"type":"error","msg":"Room not found"})
                elif len(room["sockets"]) >= 2:
                    await tx(ws, {"type":"error","msg":"Room is full"})
                else:
                    room["sockets"].add(ws)
                    room["players"][id(ws)] = 2
                    ws_to_room[id(ws)] = code
                    await tx(ws, {"type":"room_joined","room_code":code,"player_id":2})
                    await broadcast(code, {"type":"player_joined","player_id":2}, exclude=ws)
                    await broadcast(code, {"type":"start_game"})
                    print(f"[WS] P2 joined room {code}")

            elif t == "solo":
                code = make_code()
                rooms[code] = {"sockets":{ws},"players":{id(ws):1},"solo":True}
                ws_to_room[id(ws)] = code
                await tx(ws, {"type":"room_created","room_code":code,"player_id":1})
                await tx(ws, {"type":"start_game"})

            elif t == "chat":
                code = ws_to_room.get(id(ws))
                if code:
                    pid = rooms[code]["players"].get(id(ws),1)
                    await broadcast(code,{"type":"chat","player_id":pid,
                        "text":str(msg.get("text",""))[:80],"ts":time.time()})
            else:
                code = ws_to_room.get(id(ws))
                if code:
                    msg["from_player"] = rooms[code]["players"].get(id(ws),1)
                    await broadcast(code, msg, exclude=ws)

    finally:
        await drop(ws)

    return ws


def main():
    app = web.Application()
    app.router.add_get("/",  index)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/index.html", index)
    print(f"[Server] Starting on port {PORT}")
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
