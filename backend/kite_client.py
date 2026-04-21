from kiteconnect import KiteConnect, KiteTicker
import logging
import time
import threading
import pandas as pd

class KiteClient:
    def __init__(self, api_key: str, api_secret: str, access_token: str = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.kite = None
        self.access_token = access_token

        # WebSocket ticker
        self.kws = None
        self._ticker_thread = None

        # Instrument token lookup: { "NSE:INFY": 408065, "BSE:INFY": 128083204, ... }
        self._instrument_map = {}
        # Reverse map: { 408065: "NSE:INFY", ... }
        self._token_to_symbol = {}

        # Thread-safe latest prices pushed by KiteTicker
        self._prices_lock = threading.Lock()
        self._latest_prices = {}  # { "NSE:INFY": 1842.5, "BSE:INFY": 1843.0, ... }

        # Cached is_connected result
        self._connected_cache = False
        self._connected_cache_time = 0
        self._CONNECTED_TTL = 30  # seconds

        # Subscribers waiting for price updates (WebSocket clients)
        self._ws_subscribers = []
        self._ws_lock = threading.Lock()

        try:
            self.kite = KiteConnect(api_key=self.api_key)
            if self.access_token:
                self.kite.set_access_token(self.access_token)
        except Exception as e:
            logging.error(f"Kite initialization failed: {e}")

    # ---------- Connection ----------

    def get_login_url(self):
        if self.kite:
            return self.kite.login_url()
        return ""

    def generate_session(self, request_token: str):
        if not self.kite:
            return None
        data = self.kite.generate_session(request_token, api_secret=self.api_secret)
        self.access_token = data["access_token"]
        self.kite.set_access_token(self.access_token)
        # Refresh connection cache immediately
        self._connected_cache = True
        self._connected_cache_time = time.time()
        return self.access_token

    def is_connected(self):
        """Cached connection check — only calls kite.margins() every 30 seconds."""
        now = time.time()
        if now - self._connected_cache_time < self._CONNECTED_TTL:
            return self._connected_cache
        try:
            if not self.access_token or not self.kite:
                self._connected_cache = False
                self._connected_cache_time = now
                return False
            self.kite.margins()
            self._connected_cache = True
            self._connected_cache_time = now
            return True
        except Exception:
            self._connected_cache = False
            self._connected_cache_time = now
            return False

    # ---------- Instrument Token Resolution ----------

    def load_instruments(self):
        """Fetch instrument list from Kite and build token lookup maps. Call once at startup."""
        if not self.kite or not self.access_token:
            logging.warning("Cannot load instruments: not connected yet.")
            return

        try:
            nse_instruments = self.kite.instruments("NSE")
            bse_instruments = self.kite.instruments("BSE")

            for inst in nse_instruments:
                key = f"NSE:{inst['tradingsymbol']}"
                self._instrument_map[key] = inst["instrument_token"]
                self._token_to_symbol[inst["instrument_token"]] = key

            for inst in bse_instruments:
                key = f"BSE:{inst['tradingsymbol']}"
                self._instrument_map[key] = inst["instrument_token"]
                self._token_to_symbol[inst["instrument_token"]] = key

            logging.info(f"Instrument map loaded: {len(self._instrument_map)} instruments (NSE: {len(nse_instruments)}, BSE: {len(bse_instruments)})")
        except Exception as e:
            logging.error(f"Failed to load instruments: {e}")

    def get_token(self, exchange_symbol: str):
        """Resolve 'NSE:INFY' -> instrument_token integer."""
        return self._instrument_map.get(exchange_symbol)

    def get_tokens_for_symbols(self, symbols):
        """Given a list of bare symbols like ['INFY','TCS'], return list of (token, exchange_symbol) tuples for NSE+BSE."""
        result = []
        for sym in symbols:
            for exch in ["NSE", "BSE"]:
                key = f"{exch}:{sym}"
                token = self._instrument_map.get(key)
                if token:
                    result.append((token, key))
        return result

    # ---------- WebSocket Ticker ----------

    def start_ticker(self, symbols):
        """Start KiteTicker WebSocket in a background thread for the given symbols."""
        if not self.access_token:
            logging.warning("Cannot start ticker: no access token")
            return

        token_pairs = self.get_tokens_for_symbols(symbols)
        if not token_pairs:
            logging.warning("No valid instrument tokens found for symbols")
            return

        tokens = [t[0] for t in token_pairs]
        logging.info(f"Starting KiteTicker for {len(tokens)} instruments: {[t[1] for t in token_pairs[:10]]}...")

        self.kws = KiteTicker(self.api_key, self.access_token)

        def on_ticks(ws, ticks):
            updates = {}
            with self._prices_lock:
                for tick in ticks:
                    token = tick["instrument_token"]
                    symbol_key = self._token_to_symbol.get(token)
                    if symbol_key:
                        price = tick["last_price"]
                        self._latest_prices[symbol_key] = price
                        updates[symbol_key] = price

            # Notify all WebSocket subscribers
            if updates:
                self._notify_subscribers(updates)

        def on_connect(ws, response):
            logging.info(f"KiteTicker connected. Subscribing to {len(tokens)} tokens.")
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_LTP, tokens)

        def on_close(ws, code, reason):
            logging.warning(f"KiteTicker closed: {code} - {reason}")

        def on_error(ws, code, reason):
            logging.error(f"KiteTicker error: {code} - {reason}")

        self.kws.on_ticks = on_ticks
        self.kws.on_connect = on_connect
        self.kws.on_close = on_close
        self.kws.on_error = on_error

        self._ticker_thread = threading.Thread(target=self.kws.connect, daemon=True)
        self._ticker_thread.start()

    def update_subscriptions(self, symbols):
        """Update KiteTicker subscriptions when watchlist changes."""
        token_pairs = self.get_tokens_for_symbols(symbols)
        if not token_pairs or not self.kws:
            return

        tokens = [t[0] for t in token_pairs]
        try:
            self.kws.subscribe(tokens)
            self.kws.set_mode(self.kws.MODE_LTP, tokens)
            logging.info(f"Updated subscriptions: {len(tokens)} tokens")
        except Exception as e:
            logging.error(f"Failed to update subscriptions: {e}")

    def get_latest_prices(self):
        """Return a snapshot of all latest prices from WebSocket stream."""
        with self._prices_lock:
            return dict(self._latest_prices)

    # ---------- WebSocket Subscriber Notification ----------

    def register_ws_subscriber(self, callback):
        with self._ws_lock:
            self._ws_subscribers.append(callback)

    def unregister_ws_subscriber(self, callback):
        with self._ws_lock:
            self._ws_subscribers = [s for s in self._ws_subscribers if s is not callback]

    def _notify_subscribers(self, updates):
        with self._ws_lock:
            dead = []
            for callback in self._ws_subscribers:
                try:
                    callback(updates)
                except Exception:
                    dead.append(callback)
            for d in dead:
                self._ws_subscribers.remove(d)

    # ---------- REST API Methods (fallback / orders) ----------

    def get_quote(self, instruments):
        if not self.kite:
            return None
        try:
            return self.kite.quote(instruments)
        except Exception as e:
            logging.error(f"Error fetching quote: {e}")
            return None

    def get_holdings(self):
        if not self.kite:
            return None
        try:
            return self.kite.holdings()
        except Exception as e:
            logging.error(f"Error fetching holdings: {e}")
            return None

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type="MARKET", product="MIS", price: float = None):
        if not self.kite:
            return None
        try:
            order_params = {
                "tradingsymbol": tradingsymbol,
                "exchange": exchange,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "order_type": order_type,
                "product": product,
                "variety": self.kite.VARIETY_REGULAR,
                "validity": self.kite.VALIDITY_DAY
            }
            if price is not None:
                order_params["price"] = price

            return self.kite.place_order(**order_params)
        except Exception as e:
            logging.error(f"Error placing order: {e}")
            raise e

    def get_positions(self):
        if not self.kite:
            return None
        try:
            return self.kite.positions()
        except Exception as e:
            logging.error(f"Error fetching positions: {e}")
            return None

    def get_orders(self):
        if not self.kite:
            return None
        try:
            return self.kite.orders()
        except Exception as e:
            logging.error(f"Error fetching orders: {e}")
            return None
