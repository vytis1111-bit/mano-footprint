import asyncio
import json
import os
import websockets
import websockets.client

PORT = int(os.environ.get("PORT", 8765))
CONNECTED_CLIENTS = set()

async def binance_listener():
    binance_url = "wss://fstream.binance.com/ws/paxgusdt@trade"
    while True:
        try:
            async with websockets.client.connect(binance_url) as websocket:
                async for message in websocket:
                    data = json.loads(message)
                    trade_data = {
                        "time": data.get("T", 0) / 1000,
                        "open": float(data.get("p", 0)),
                        "high": float(data.get("p", 0)),
                        "low": float(data.get("p", 0)),
                        "close": float(data.get("p", 0)),
                        "buy": float(data.get("q", 0)) if not data.get("m") else 0,
                        "sell": float(data.get("q", 0)) if data.get("m") else 0,
                        "delta": float(data.get("q", 0)) * (-1 if data.get("m") else 1),
                        "poc": float(data.get("p", 0))
                    }
                    
                    if CONNECTED_CLIENTS:
                        payload = json.dumps(trade_data)
                        await asyncio.gather(
                            *[client.send(payload) for client in CONNECTED_CLIENTS],
                            return_exceptions=True
                        )
        except Exception as e:
            print(f"Binance connection error: {e}")
            await asyncio.sleep(5)

async def web_handler(websocket):
    CONNECTED_CLIENTS.add(websocket)
    try:
        await asyncio.Future()
    finally:
        CONNECTED_CLIENTS.remove(websocket)

async def main():
    asyncio.create_task(binance_listener())
    async with websockets.serve(web_handler, "0.0.0.0", PORT):
        print(f"Server started on port {PORT}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
