from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from .kite_client import KiteClient
from .algo import ArbitrageAlgo
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
import logging
import json
import asyncio

# Configure Logging
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Arbitrage Algo API")

# Setup CORS for local UI testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Credentials
API_KEY = os.environ.get("KITE_API_KEY", "b5012qtc6pcpbzhb")
API_SECRET = os.environ.get("KITE_API_SECRET", "e6b8ivxfk760md5qtgt8ms3xmc20wves")
ACCESS_TOKEN = os.environ.get("KITE_ACCESS_TOKEN", None)

kite_client = KiteClient(API_KEY, API_SECRET, ACCESS_TOKEN)
algo = ArbitrageAlgo(kite_client)

class TradeRequest(BaseModel):
    symbol: str
    buy_exchange: str
    buy_price: float
    sell_exchange: str
    sell_price: float
    quantity: int = 1

from fastapi.responses import RedirectResponse

@app.on_event("startup")
def startup_event():
    """Load instrument token map at server boot if already connected."""
    if kite_client.is_connected():
        logging.info("Connected at startup — loading instrument map...")
        kite_client.load_instruments()

@app.get("/api/login")
def login_redirect():
    url = kite_client.get_login_url()
    if url:
        return RedirectResponse(url)
    raise HTTPException(status_code=500, detail="Failed to retrieve login URL")

@app.get("/redirect")
def login_callback(request_token: str):
    try:
        token = kite_client.generate_session(request_token)
        # Load instruments after fresh login
        kite_client.load_instruments()
        return {"status": "success", "access_token": token, "message": "Successfully authenticated with Kite. You can now close this tab and return to the UI."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/status")
def get_status():
    return {
        "connected": kite_client.is_connected(),
        "algo_running": algo.is_running,
        "stop_requested": algo.stop_requested
    }

@app.post("/api/trade/once")
def trade_once(req: TradeRequest):
    if algo.stop_requested:
        raise HTTPException(status_code=400, detail="Emergency stop is active. Reset required.")
    
    result = algo.execute_single_trade(
        req.symbol, 
        req.buy_exchange, 
        req.buy_price, 
        req.sell_exchange, 
        req.sell_price, 
        req.quantity
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@app.post("/api/trade/stop")
def stop_algo():
    return algo.trigger_emergency_stop()

@app.post("/api/trade/reset")
def reset_algo():
    algo.stop_requested = False
    return {"status": "success", "message": "Algo limits reset."}

@app.get("/api/quote")
def get_quote(symbol: str):
    if not kite_client.is_connected():
        return {"status": "error", "message": "Kite API not connected", "nse": 0, "bse": 0}
    
    # Try WebSocket cache first
    prices = kite_client.get_latest_prices()
    nse_key = f"NSE:{symbol}"
    bse_key = f"BSE:{symbol}"
    
    nse_price = prices.get(nse_key, 0)
    bse_price = prices.get(bse_key, 0)
    
    # Fallback to REST if no cached data
    if nse_price == 0 and bse_price == 0:
        quotes = kite_client.get_quote([nse_key, bse_key])
        if quotes:
            nse_price = quotes.get(nse_key, {}).get('last_price', 0)
            bse_price = quotes.get(bse_key, {}).get('last_price', 0)
    
    return {
        "status": "success",
        "symbol": symbol,
        "nse": nse_price,
        "bse": bse_price
    }

@app.get("/api/quotes/bulk")
def get_bulk_quotes(symbols: List[str] = Query(...)):
    """Fetch NSE and BSE prices for multiple symbols — uses WebSocket cache with REST fallback."""
    if not kite_client.is_connected():
        return {"status": "error", "message": "Kite API not connected", "data": {}}

    prices = kite_client.get_latest_prices()
    result = {}
    missing_instruments = []

    for sym in symbols:
        nse_key = f"NSE:{sym}"
        bse_key = f"BSE:{sym}"
        nse_price = prices.get(nse_key, 0)
        bse_price = prices.get(bse_key, 0)
        result[sym] = {"nse": nse_price, "bse": bse_price}

        # Track symbols missing from WebSocket cache for REST fallback
        if nse_price == 0:
            missing_instruments.append(nse_key)
        if bse_price == 0:
            missing_instruments.append(bse_key)

    # REST fallback for any missing prices
    if missing_instruments:
        try:
            quotes = kite_client.get_quote(missing_instruments)
            if quotes:
                for sym in symbols:
                    nse_key = f"NSE:{sym}"
                    bse_key = f"BSE:{sym}"
                    if result[sym]["nse"] == 0:
                        result[sym]["nse"] = quotes.get(nse_key, {}).get("last_price", 0)
                    if result[sym]["bse"] == 0:
                        result[sym]["bse"] = quotes.get(bse_key, {}).get("last_price", 0)
        except Exception as e:
            logging.error(f"REST fallback failed: {e}")

    return {"status": "success", "data": result}


# ---------- WebSocket Endpoint for Real-Time Price Streaming ----------

# Track active WebSocket connections
_active_ws_connections: List[WebSocket] = []

@app.websocket("/ws/prices")
async def ws_prices(websocket: WebSocket):
    await websocket.accept()
    
    # Queue for this specific connection
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    def on_price_update(updates):
        """Called from KiteTicker thread — schedule onto asyncio event loop."""
        try:
            loop.call_soon_threadsafe(queue.put_nowait, updates)
        except Exception:
            pass

    kite_client.register_ws_subscriber(on_price_update)

    try:
        # Handle incoming messages (subscription changes) and outgoing price pushes concurrently
        async def send_prices():
            while True:
                updates = await queue.get()
                try:
                    await websocket.send_json(updates)
                except Exception:
                    break

        async def receive_messages():
            while True:
                try:
                    data = await websocket.receive_json()
                    # Client sends: {"action": "subscribe", "symbols": ["INFY","TCS",...]}
                    if data.get("action") == "subscribe":
                        symbols = data.get("symbols", [])
                        if symbols:
                            kite_client.update_subscriptions(symbols)
                            # Also start ticker if not already running
                            if not kite_client.kws or not kite_client._ticker_thread or not kite_client._ticker_thread.is_alive():
                                kite_client.start_ticker(symbols)
                            else:
                                kite_client.update_subscriptions(symbols)
                except WebSocketDisconnect:
                    break
                except Exception:
                    break

        await asyncio.gather(send_prices(), receive_messages())

    except WebSocketDisconnect:
        pass
    finally:
        kite_client.unregister_ws_subscriber(on_price_update)


@app.get("/api/investments")
def get_investments():
    if not kite_client.is_connected():
        return {"status": "error", "message": "Kite API not connected"}
    holdings = kite_client.get_holdings()
    if holdings is None:
        return {"status": "error", "message": "Failed to fetch holdings"}
    return {"status": "success", "data": holdings}

@app.get("/api/pnl")
def get_pnl():
    if not kite_client.is_connected():
        return {"status": "error", "message": "Kite API not connected"}
    positions = kite_client.get_positions()
    if positions is None:
        return {"status": "error", "message": "Failed to fetch positions"}
    # The positions endpoint returns both 'net' and 'day'.
    day_positions = positions.get("day", [])
    
    # Calculate some summary stats
    total_m2m = sum((p.get("m2m", 0) for p in day_positions))
    total_realised = sum((p.get("realised", 0) for p in day_positions))
    total_unrealised = sum((p.get("unrealised", 0) for p in day_positions))

    return {
        "status": "success", 
        "data": {
            "positions": day_positions,
            "summary": {
                "m2m": total_m2m,
                "realised": total_realised,
                "unrealised": total_unrealised
            }
        }
    }

@app.get("/api/orders")
def get_orders():
    if not kite_client.is_connected():
        return {"status": "error", "message": "Kite API not connected"}
    orders = kite_client.get_orders()
    if orders is None:
        return {"status": "error", "message": "Failed to fetch orders"}
    return {"status": "success", "data": orders}

# Mount the frontend directory if it exists
frontend_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.exists(frontend_path):
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")

    @app.get("/")
    def serve_index():
        return FileResponse(os.path.join(frontend_path, "index.html"))

    @app.get("/{path:path}")
    def serve_static(path: str):
        file_path = os.path.join(frontend_path, path)
        if os.path.exists(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(frontend_path, "index.html"))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
