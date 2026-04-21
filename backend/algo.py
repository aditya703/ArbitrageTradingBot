import time
import logging
from concurrent.futures import ThreadPoolExecutor
from .kite_client import KiteClient

class ArbitrageAlgo:
    def __init__(self, kite_client: KiteClient):
        self.kite = kite_client
        self.is_running = False
        self.stop_requested = False
        # Reusable thread pool for concurrent order placement
        self._executor = ThreadPoolExecutor(max_workers=2)

    def trigger_emergency_stop(self):
        """Immediately sets flags to halt further loop iterations and orders."""
        self.stop_requested = True
        self.is_running = False
        return {"status": "success", "message": "EMERGENCY STOP ACTIVATED."}

    def execute_single_trade(self, symbol: str, buy_exchange: str, buy_price: float, sell_exchange: str, sell_price: float, quantity: int = 1):
        """
        Executes a Limit Buy and a Limit Sell order CONCURRENTLY using ThreadPoolExecutor.
        Both orders fire simultaneously to minimize slippage.
        """
        start_time = time.time()
        
        if self.stop_requested:
            return {"status": "error", "message": "Algo stopped by emergency kill switch."}
        
        if not self.kite.is_connected():
            return {"status": "error", "message": "Kite API not connected or invalid tokens. Edit main.py with your keys."}

        self.is_running = True

        try:
            # Fire both orders concurrently
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

            msg = f"Orders Placed! BUY [{buy_exchange}]: {buy_order_id or f'FAILED ({buy_error})'} | SELL [{sell_exchange}]: {sell_order_id or f'FAILED ({sell_error})'} (Took: {total_time}ms)"
            return {"status": "success", "message": msg, "latency_ms": total_time}

        except Exception as e:
            self.is_running = False
            return {"status": "error", "message": f"Execution failed: {e}"}
