"""
server.py - Space Invaders Co-op + 1v1
Supports: Classic Co-op, Boss Rush Co-op, 1v1 PvP
Multiple room sizes (2-4 players)
"""
import asyncio, websockets.legacy.server as wslib
from websockets.exceptions import ConnectionClosed
import json, random, string, time, os
from http import HTTPStatus

PORT = int(os.environ.get("PORT", 10000))
rooms = {}      # code -> {players:{ws:pid}, mode, max_players, state}
ws_to_room = {} # ws -> code

def make_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))

async def broadcast(code, msg, exclude=None):
    room = rooms.get(code)
    if not room: return
    data = json.dumps(msg)
    for ws in list(room["players"]):
        if ws is exclude: continue
        try: await ws.send(data)
        except: pass

async def tx(ws, msg):
    try: await ws.send(json.dumps(msg))
    except: pass

async def drop(ws):
    code = ws_to_room.pop(ws, None)
    if not code: return
    room = rooms.get(code)
    if room:
        pid = room["players"].pop(ws, None)
        if pid: await broadcast(code, {"type":"player_left","player_id":pid})
        if not room["players"]: rooms.pop(code, None)

async def handler(ws, path="/"):
    try:
        async for raw in ws:
            try: msg = json.loads(raw)
            except: continue
            t = msg.get("type")

            if t == "create_room":
                code = make_code()
                mode = msg.get("mode", "coop")
                max_players = msg.get("max_players", 2)
                rooms[code] = {"players":{ws:1}, "mode":mode, "max_players":max_players, "started":False}
                ws_to_room[ws] = code
                await tx(ws, {"type":"room_created","room_code":code,"player_id":1,"mode":mode,"max_players":max_players})
                print(f"[WS] Room {code} created mode={mode} max={max_players}")

            elif t == "join_room":
                code = msg.get("room_code","").upper().strip()
                room = rooms.get(code)
                if not room:
                    await tx(ws, {"type":"error","msg":"Room not found"})
                elif len(room["players"]) >= room["max_players"]:
                    await tx(ws, {"type":"error","msg":"Room is full"})
                else:
                    pid = len(room["players"]) + 1
                    room["players"][ws] = pid
                    ws_to_room[ws] = code
                    await tx(ws, {"type":"room_joined","room_code":code,"player_id":pid,"mode":room["mode"],"max_players":room["max_players"]})
                    await broadcast(code, {"type":"player_joined","player_id":pid,"current_count":len(room["players"])}, exclude=ws)
                    # Start when full
                    if len(room["players"]) >= room["max_players"]:
                        room["started"] = True
                        await broadcast(code, {"type":"start_game","mode":room["mode"],"player_count":len(room["players"])})
                        print(f"[WS] Room {code} started!")

            elif t == "solo":
                code = make_code()
                rooms[code] = {"players":{ws:1},"mode":"solo","max_players":1,"started":True}
                ws_to_room[ws] = code
                await tx(ws, {"type":"room_created","room_code":code,"player_id":1})
                await tx(ws, {"type":"start_game","mode":"solo","player_count":1})

            elif t == "chat":
                code = ws_to_room.get(ws)
                if code:
                    pid = rooms[code]["players"].get(ws,1)
                    await broadcast(code, {"type":"chat","player_id":pid,"text":str(msg.get("text",""))[:80],"ts":time.time()})

            # Fast relay for time-sensitive messages
            elif t in ("pvp_hit", "pvp_dead", "bullet_fired", "player_update"):
                code = ws_to_room.get(ws)
                if code:
                    pid = rooms[code]["players"].get(ws,1)
                    msg["from_player"] = pid
                    msg["player_id"] = pid
                    await broadcast(code, msg, exclude=ws)

            else:
                code = ws_to_room.get(ws)
                if code:
                    pid = rooms[code]["players"].get(ws,1)
                    msg["from_player"] = pid
                    await broadcast(code, msg, exclude=ws)

    except ConnectionClosed:
        pass
    finally:
        await drop(ws)

def serve_html(path, request_headers):
    if request_headers.get("Upgrade","").lower() == "websocket":
        return None
    try:
        with open("index.html","rb") as f: body = f.read()
        return HTTPStatus.OK, [("Content-Type","text/html; charset=utf-8"),("Content-Length",str(len(body))),("Cache-Control","no-cache")], body
    except FileNotFoundError:
        body = b"<h1>index.html not found</h1>"
        return HTTPStatus.NOT_FOUND, [("Content-Type","text/html"),("Content-Length",str(len(body)))], body

async def main():
    print(f"[Server] Starting on port {PORT}")
    async with wslib.serve(handler, "0.0.0.0", PORT, process_request=serve_html):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
