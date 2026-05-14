import os, json, random, string
from aiohttp import web, WSMsgType

PORT = int(os.environ.get("PORT", 10000))

# Game rooms
rooms = {}
ws_info = {}

# Friends/presence system
named_players = {}   # name -> ws
ws_names = {}        # ws -> name

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

async def notify_friends_online(name):
    """Tell all of this player's friends that they came online."""
    # We broadcast to everyone who has this name in their friends list
    for ws, wname in list(ws_names.items()):
        if wname != name:
            await tx(ws, {"type": "friend_online", "name": name})

async def notify_friends_offline(name):
    for ws, wname in list(ws_names.items()):
        if wname != name:
            await tx(ws, {"type": "friend_offline", "name": name})

async def cleanup(ws):
    # Friends cleanup
    name = ws_names.pop(ws, None)
    if name:
        named_players.pop(name, None)
        await notify_friends_offline(name)

    # Game cleanup
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
            t = d.get("type", "")

            # ── PRESENCE / FRIENDS ──
            if t == "register":
                name = str(d.get("name","")).upper().strip()[:16]
                if not name: continue
                if name in named_players and named_players[name] is not ws:
                    await tx(ws, {"type":"name_taken"})
                    continue
                named_players[name] = ws
                ws_names[ws] = name
                await notify_friends_online(name)

            elif t == "friends_list":
                # Client tells us their friends — send back who is online
                flist = d.get("friends", [])
                for fname in flist:
                    fname = str(fname).upper().strip()
                    if fname in named_players:
                        await tx(ws, {"type":"friend_online","name":fname})

            elif t == "add_friend":
                name = str(d.get("name","")).upper().strip()
                if name in named_players:
                    await tx(ws, {"type":"friend_online","name":name})

            elif t == "friend_msg":
                to_name = str(d.get("to","")).upper().strip()
                text = str(d.get("text",""))[:200]
                from_name = ws_names.get(ws, "UNKNOWN")
                target_ws = named_players.get(to_name)
                if target_ws:
                    await tx(target_ws, {"type":"friend_msg","from":from_name,"text":text})
                else:
                    await tx(ws, {"type":"friend_msg_failed","reason":to_name+" is offline"})

            elif t == "ping":
                await tx(ws, {"type":"pong"})

            # ── GAME ROOMS ──
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
                if info:
                    name = ws_names.get(ws, "P"+str(info["pid"]))
                    await broadcast(info["code"],{"type":"chat","player_id":info["pid"],"name":name,"text":str(d.get("text",""))[:80]})

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
