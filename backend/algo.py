import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from .kite_client import KiteClient

class ArbitrageAlgo:
    def __init__(self, kite_client: KiteClient):
        self.kite = kite_client
        self.is_running = False
        self.stop_requested = False
        # Expanded thread pool: 2 threads per trade × up to 4 simultaneous symbols = 8
        self._executor = ThreadPoolExecutor(max_workers=8)

        # Cached M2M — avoid a blocking REST call on every single trade
        self._cached_m2m = 0
        self._m2m_check_time = 0
        self._M2M_CACHE_TTL = 1.0  # seconds — check at most once per second

    def trigger_emergency_stop(self):
        """Immediately sets flags to halt further loop iterations and orders."""
        self.stop_requested = True
        self.is_running = False
        return {"status": "success", "message": "EMERGENCY STOP ACTIVATED."}

    def _get_m2m_cached(self):
        """Check daily M2M at most once per second to avoid blocking every trade."""
        now = time.time()
        if now - self._m2m_check_time < self._M2M_CACHE_TTL:
            return self._cached_m2m

        positions = self.kite.get_positions()
        if positions is not None:
            day_positions = positions.get("day", [])
            self._cached_m2m = sum((p.get("m2m", 0) for p in day_positions))
        self._m2m_check_time = now
        return self._cached_m2m

    def execute_single_trade(self, symbol: str, buy_exchange: str, buy_price: float, sell_exchange: str, sell_price: float, quantity: int = 1):
        """
        Executes a LIMIT IOC Buy and a LIMIT IOC Sell order CONCURRENTLY.
        Both orders fire simultaneously to minimize slippage.
        After placement, a background monitor auto-handles stuck/one-legged orders.
        """
        start_time = time.time()
        
        if self.stop_requested:
            return {"status": "error", "message": "Algo stopped by emergency kill switch."}
        
        if not self.kite.is_connected():
            return {"status": "error", "message": "Kite API not connected or invalid tokens. Edit main.py with your keys."}

        # Guardrail 1: Hard Stop-Loss Check (cached — at most 1 REST call/second)
        total_m2m = self._get_m2m_cached()
        if total_m2m <= -5.0:
            self.trigger_emergency_stop()
            return {"status": "error", "message": f"EMERGENCY STOP: Daily M2M is ₹{total_m2m:.2f} (exceeds -₹5 limit)."}

        # Guardrail 2: Margin/Funds Check
        # Assuming 5x leverage for MIS: required cash is 20% of total exposure.
        # Since we place both Buy and Sell simultaneously, both require margin initially.
        exposure = (buy_price + sell_price) * quantity
        required_margin = exposure / 5.0 
        available_margin = self.kite.get_available_margin()
        
        if available_margin < required_margin:
             return {"status": "error", "message": f"INSUFFICIENT FUNDS: Need ₹{required_margin:.2f} but only have ₹{available_margin:.2f} available."}

        self.is_running = True

        try:
            # Fire both orders concurrently (LIMIT + IOC validity is set automatically in kite_client)
            buy_future = self._executor.submit(
                self.kite.place_order,
                tradingsymbol=symbol,
                exchange=buy_exchange,
                transaction_type="BUY",
                quantity=quantity,
                order_type="LIMIT",
                product="MIS",
                price=buy_price
            )

            sell_future = self._executor.submit(
                self.kite.place_order,
                tradingsymbol=symbol,
                exchange=sell_exchange,
                transaction_type="SELL",
                quantity=quantity,
                order_type="LIMIT",
                product="MIS",
                price=sell_price
            )

            # Wait for both to complete
            buy_order_id = None
            sell_order_id = None
            buy_error = None
            sell_error = None

            try:
                buy_order_id = buy_future.result(timeout=10)
            except Exception as e:
                buy_error = str(e)
                logging.error(f"BUY order failed: {e}")

            try:
                sell_order_id = sell_future.result(timeout=10)
            except Exception as e:
                sell_error = str(e)
                logging.error(f"SELL order failed: {e}")

            self.is_running = False
            total_time = round((time.time() - start_time) * 1000, 2)

            if not buy_order_id and not sell_order_id:
                return {"status": "error", "message": f"Failed to place both orders. BUY: {buy_error}, SELL: {sell_error}"}

            logging.info(f"CONCURRENT EXECUTION: Total: {total_time}ms | BUY: {buy_order_id} | SELL: {sell_order_id}")

            # Launch background monitor for stale/one-legged order cleanup
            if buy_order_id or sell_order_id:
                monitor_thread = threading.Thread(
                    target=self._monitor_and_cleanup,
                    args=(symbol, buy_exchange, sell_exchange, buy_order_id, sell_order_id, quantity),
                    daemon=True
                )
                monitor_thread.start()

            msg = f"IOC Orders Placed! BUY [{buy_exchange}]: {buy_order_id or f'FAILED ({buy_error})'} | SELL [{sell_exchange}]: {sell_order_id or f'FAILED ({sell_error})'} (Took: {total_time}ms)"
            return {"status": "success", "message": msg, "latency_ms": total_time}

        except Exception as e:
            self.is_running = False
            return {"status": "error", "message": f"Execution failed: {e}"}

    def _monitor_and_cleanup(self, symbol, buy_exchange, sell_exchange, buy_order_id, sell_order_id, quantity):
        """
        Background thread: monitors IOC order fill status after 2 seconds.
        Handles one-legged positions by squaring off the filled side.
        
        Timeline:
          T+2s  → Check fill status. If one leg filled but the other didn't,
                  squareoff the filled leg with a MARKET order.
          T+5s  → Cancel any remaining OPEN orders as a safety sweep.
        """
        try:
            time.sleep(2)  # Give IOC orders time to process (should be instant, but network latency exists)

            if self.stop_requested:
                logging.warning(f"[CLEANUP] Emergency stop active — skipping cleanup for {symbol}")
                return

            orders = self.kite.get_orders()
            if not orders:
                logging.warning(f"[CLEANUP] Could not fetch orders for {symbol}")
                return

            # Find our specific orders
            buy_status = None
            sell_status = None
            for o in orders:
                if buy_order_id and str(o.get("order_id")) == str(buy_order_id):
                    buy_status = o
                if sell_order_id and str(o.get("order_id")) == str(sell_order_id):
                    sell_status = o

            buy_filled = buy_status and buy_status.get("status") == "COMPLETE"
            sell_filled = sell_status and sell_status.get("status") == "COMPLETE"
            buy_qty_filled = buy_status.get("filled_quantity", 0) if buy_status else 0
            sell_qty_filled = sell_status.get("filled_quantity", 0) if sell_status else 0

            if buy_filled and sell_filled:
                logging.info(f"[CLEANUP] ✅ Both legs filled for {symbol}. Arbitrage trade complete.")
                return

            if not buy_filled and not sell_filled:
                logging.info(f"[CLEANUP] Both IOC orders cancelled/unfilled for {symbol}. No position taken — clean exit.")
                return

            # ONE-LEGGED SITUATION: One filled, the other didn't
            # Squareoff the filled leg immediately with a MARKET order
            if buy_filled and not sell_filled:
                logging.warning(f"[CLEANUP] ⚠️ ONE-LEGGED for {symbol}: BUY filled ({buy_qty_filled} shares) but SELL didn't. Squaring off...")
                try:
                    squareoff_id = self.kite.place_order(
                        tradingsymbol=symbol,
                        exchange=buy_exchange,
                        transaction_type="SELL",
                        quantity=buy_qty_filled,
                        order_type="MARKET",
                        product="MIS"
                    )
                    logging.warning(f"[CLEANUP] Squareoff SELL placed for {symbol}: order_id={squareoff_id}")
                except Exception as e:
                    logging.error(f"[CLEANUP] CRITICAL: Failed to squareoff {symbol} BUY leg: {e}")

            elif sell_filled and not buy_filled:
                logging.warning(f"[CLEANUP] ⚠️ ONE-LEGGED for {symbol}: SELL filled ({sell_qty_filled} shares) but BUY didn't. Squaring off...")
                try:
                    squareoff_id = self.kite.place_order(
                        tradingsymbol=symbol,
                        exchange=sell_exchange,
                        transaction_type="BUY",
                        quantity=sell_qty_filled,
                        order_type="MARKET",
                        product="MIS"
                    )
                    logging.warning(f"[CLEANUP] Squareoff BUY placed for {symbol}: order_id={squareoff_id}")
                except Exception as e:
                    logging.error(f"[CLEANUP] CRITICAL: Failed to squareoff {symbol} SELL leg: {e}")

            # Safety sweep: cancel any orders that are still OPEN (shouldn't happen with IOC, but belt-and-suspenders)
            time.sleep(3)
            try:
                orders_refresh = self.kite.get_orders()
                if orders_refresh:
                    for o in orders_refresh:
                        oid = str(o.get("order_id"))
                        if oid in [str(buy_order_id), str(sell_order_id)] and o.get("status") in ["OPEN", "PENDING", "TRIGGER PENDING"]:
                            logging.warning(f"[CLEANUP] Cancelling stale order {oid} for {symbol}")
                            self.kite.cancel_order(oid)
            except Exception as e:
                logging.error(f"[CLEANUP] Error in safety sweep for {symbol}: {e}")

        except Exception as e:
            logging.error(f"[CLEANUP] Monitor thread crashed for {symbol}: {e}")
