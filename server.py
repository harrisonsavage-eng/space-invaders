"""
server.py - Space Invaders Co-op
Serves index.html over HTTP AND handles WebSocket connections.
Single service = one Render deployment, one URL.

Install: pip install -r requirements.txt
Run:     python server.py
"""

import asyncio
import websockets
import json
import random
import time
import os
import http.server
import threading
import string
from http.server import HTTPServer, SimpleHTTPRequestHandler

PORT = int(os.environ.get("PORT", 10000))

# ─────────────────────────────────────────────
# HTTP SERVER (serves index.html)
# ─────────────────────────────────────────────

class GameHTTPHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/' or self.path == '/index.html':
            self.path = '/index.html'
        return super().do_GET()

    def log_message(self, format, *args):
        pass  # suppress HTTP logs


def start_http_server():
    """Serve static files on PORT+1 (internal only, proxied by websocket server)."""
    handler = GameHTTPHandler
    httpd = HTTPServer(('0.0.0.0', PORT + 1), handler)
    print(f"[HTTP] Serving files on port {PORT + 1}")
    httpd.serve_forever()


# ─────────────────────────────────────────────
# WEBSOCKET GAME SERVER
# ─────────────────────────────────────────────

rooms = {}       # room_code -> { players:{ws->pid}, solo:bool }
ws_to_room = {}  # ws -> room_code


def make_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))


def new_room():
    return {"players": {}, "solo": False, "state": {}}


async def broadcast(room_code, msg, exclude=None):
    room = rooms.get(room_code)
    if not room:
        return
    data = json.dumps(msg)
    for ws in list(room["players"]):
        if ws is exclude:
            continue
        try:
            await ws.send(data)
        except Exception:
            pass


async def send_to(ws, msg):
    try:
        await ws.send(json.dumps(msg))
    except Exception:
        pass


async def remove_player(ws):
    code = ws_to_room.pop(ws, None)
    if not code:
        return
    room = rooms.get(code)
    if room:
        pid = room["players"].pop(ws, None)
        if pid:
            await broadcast(code, {"type": "player_left", "player_id": pid})
        if not room["players"]:
            rooms.pop(code, None)
            print(f"[WS] Room {code} closed")


async def ws_handler(ws):
    print(f"[WS] Connected: {ws.remote_address}")
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            t = msg.get("type")

            if t == "create_room":
                code = make_code()
                rooms[code] = new_room()
                rooms[code]["players"][ws] = 1
                ws_to_room[ws] = code
                await send_to(ws, {"type": "room_created", "room_code": code, "player_id": 1})
                print(f"[WS] Room {code} created")

            elif t == "join_room":
                code = msg.get("room_code", "").upper().strip()
                room = rooms.get(code)
                if not room:
                    await send_to(ws, {"type": "error", "msg": "Room not found"})
                elif len(room["players"]) >= 2:
                    await send_to(ws, {"type": "error", "msg": "Room is full"})
                else:
                    room["players"][ws] = 2
                    ws_to_room[ws] = code
                    await send_to(ws, {"type": "room_joined", "room_code": code, "player_id": 2})
                    await broadcast(code, {"type": "player_joined", "player_id": 2}, exclude=ws)
                    await broadcast(code, {"type": "start_game"})
                    print(f"[WS] P2 joined room {code}")

            elif t == "solo":
                code = make_code()
                rooms[code] = new_room()
                rooms[code]["players"][ws] = 1
                rooms[code]["solo"] = True
                ws_to_room[ws] = code
                await send_to(ws, {"type": "room_created", "room_code": code, "player_id": 1})
                await send_to(ws, {"type": "start_game"})

            elif t == "chat":
                code = ws_to_room.get(ws)
                if code:
                    pid = rooms[code]["players"].get(ws, 1)
                    await broadcast(code, {
                        "type": "chat",
                        "player_id": pid,
                        "text": str(msg.get("text", ""))[:80],
                        "ts": time.time()
                    })

            else:
                # Relay everything else to the other player
                code = ws_to_room.get(ws)
                if code:
                    pid = rooms[code]["players"].get(ws, 1)
                    msg["from_player"] = pid
                    await broadcast(code, msg, exclude=ws)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await remove_player(ws)
        print(f"[WS] Disconnected: {ws.remote_address}")


# ─────────────────────────────────────────────
# COMBINED SERVER (HTTP + WS on same port)
# Uses websockets process_request to serve HTML
# ─────────────────────────────────────────────

async def process_request(connection, request):
    """Serve index.html for normal HTTP requests, upgrade for WebSocket."""
    path = request.path.split('?')[0]
    if path in ('/', '/index.html', ''):
        try:
            with open('index.html', 'rb') as f:
                body = f.read()
            headers = [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Content-Length", str(len(body))),
            ]
            return connection.respond(http.HTTPStatus.OK, headers=headers, body=body)
        except FileNotFoundError:
            body = b"<h1>index.html not found</h1>"
            headers = [("Content-Type", "text/html"), ("Content-Length", str(len(body)))]
            return connection.respond(http.HTTPStatus.NOT_FOUND, headers=headers, body=body)
    # Return None to allow WebSocket upgrade
    return None


async def main():
    print(f"[Server] Starting on port {PORT}")
    print(f"[Server] Open http://localhost:{PORT} to play")
    async with websockets.serve(
        ws_handler,
        "0.0.0.0",
        PORT,
        process_request=process_request,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
