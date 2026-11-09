import asyncio
import json
import websockets
import websocket
import threading

CONNECTED_CLIENTS = set()
CANDLE_DURATION = 300  
TICK_SIZE = 0.10

current_candle_start = 0
footprint = {}
buy_vol = 0.0
sell_vol = 0.0
open_p = high_p = low_p = close_p = 0.0

def broadcast(data):
    msg = json.dumps(data)
    for ws in list(CONNECTED_CLIENTS):
        asyncio.run_coroutine_threadsafe(ws.send(msg), loop)

def on_message(ws, message):
    global current_candle_start, footprint, buy_vol, sell_vol, open_p, high_p, low_p, close_p
    d = json.loads(message)
    p, q, is_sell, ts = float(d['p']), float(d['q']), d['m'], d['E'] / 1000.0
    
    bucket = int(ts // CANDLE_DURATION) * CANDLE_DURATION
    if current_candle_start == 0:
        current_candle_start = bucket
        open_p = high_p = low_p = close_p = p

    if bucket > current_candle_start:
        current_candle_start = bucket
        footprint = {}
        buy_vol = sell_vol = 0.0
        open_p = high_p = low_p = close_p = p

    close_p = p
    high_p = max(high_p, p)
    low_p = min(low_p, p)

    p_lvl = round(round(p / TICK_SIZE) * TICK_SIZE, 2)
    if p_lvl not in footprint:
        footprint[p_lvl] = {'b': 0.0, 's': 0.0}

    if is_sell:
        footprint[p_lvl]['s'] += q
        sell_vol += q
    else:
        footprint[p_lvl]['b'] += q
        buy_vol += q

    poc = max(footprint, key=lambda x: footprint[x]['b'] + footprint[x]['s']) if footprint else p_lvl
    
    broadcast({
        "time": current_candle_start, "open": open_p, "high": high_p, "low": low_p, "close": close_p,
        "buy": round(buy_vol, 2), "sell": round(sell_vol, 2), "delta": round(buy_vol - sell_vol, 2), "poc": poc
    })

def start_ws():
    websocket.WebSocketApp("wss://fstream.binance.com/ws/xauusdt@trade", on_message=on_message).run_forever()

async def handler(ws):
    CONNECTED_CLIENTS.add(ws)
    try: await ws.wait_closed()
    finally: CONNECTED_CLIENTS.remove(ws)

async def main():
    global loop
    loop = asyncio.get_running_loop()
    threading.Thread(target=start_ws, daemon=True).start()
    async with websockets.serve(handler, "0.0.0.0", 8765):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
