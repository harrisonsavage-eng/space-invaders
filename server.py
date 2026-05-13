import os, json, random, string
from aiohttp import web, WSMsgType
PORT = int(os.environ.get("PORT", 10000))
rooms = {}
ws_info = {}
def make_code():
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
        if code not in rooms: return code
async def tx(ws, msg):
    try: await ws.send_str(json.dumps(msg))
    except: pass
async def broadcast(code, msg, exclude_pid=None):
    room = rooms.get(code)
    if not room: return
    data = json.dumps(msg)
    for pid, w in list(room["players"].items()):
        if pid == exclude_pid: continue
        try: await w.send_str(data)
        except: pass
async def cleanup(ws):
    info = ws_info.pop(ws, None)
    if not info: return
    code, pid = info["code"], info["pid"]
    room = rooms.get(code)
    if not room: return
    room["players"].pop(pid, None)
    if room["players"]: await broadcast(code, {"type":"player_left","player_id":pid})
    else: del rooms[code]
async def index(request):
    return web.FileResponse("index.html")
async def wshandler(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT: continue
            try: d = json.loads(msg.data)
            except: continue
            t = d.get("type","")
            if t == "ping": await tx(ws, {"type":"pong"})
            elif t == "create_room":
                code = make_code()
                mode = d.get("mode","coop")
                max_p = max(2, min(4, int(d.get("max_players",2))))
                rooms[code] = {"players":{1:ws},"mode":mode,"max_players":max_p}
                ws_info[ws] = {"code":code,"pid":1}
                await tx(ws, {"type":"room_created","room_code":code,"player_id":1,"mode":mode,"max_players":max_p})
            elif t == "join_room":
                code = str(d.get("room_code","")).upper().strip()
                room = rooms.get(code)
                if not room: await tx(ws, {"type":"error","msg":"Room "+code+" not found!"})
                elif len(room["players"]) >= room["max_players"]: await tx(ws, {"type":"error","msg":"Room is full!"})
                else:
                    pid = len(room["players"]) + 1
                    room["players"][pid] = ws
                    ws_info[ws] = {"code":code,"pid":pid}
                    count = len(room["players"])
                    await tx(ws, {"type":"room_joined","room_code":code,"player_id":pid,"mode":room["mode"],"max_players":room["max_players"]})
                    await broadcast(code, {"type":"player_joined","player_id":pid,"current_count":count,"max_players":room["max_players"]}, exclude_pid=pid)
                    if count >= room["max_players"]:
                        await broadcast(code, {"type":"start_game","mode":room["mode"],"player_count":count})
            elif t == "chat":
                info = ws_info.get(ws)
                if info: await broadcast(info["code"],{"type":"chat","player_id":info["pid"],"text":str(d.get("text",""))[:80]})
            else:
                info = ws_info.get(ws)
                if info:
                    d["from_player"] = info["pid"]
                    d["player_id"] = info["pid"]
                    await broadcast(info["code"], d, exclude_pid=info["pid"])
    except: pass
    finally: await cleanup(ws)
    return ws
app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/index.html", index)
app.router.add_get("/ws", wshandler)
app.router.add_get("/health", lambda r: web.Response(text="ok"))
if __name__ == "__main__":
    print("Starting on port", PORT)
    web.run_app(app, host="0.0.0.0", port=PORT, access_log=None)
