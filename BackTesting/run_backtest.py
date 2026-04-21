import pandas as pd
import numpy as np
import math
import os

def calculate_charges(buy_turnover, sell_turnover):
    """
    Calculates Zerodha MIS (Intraday Equity) strict charges.
    """
    # 1. Brokerage: min(0.03%, Rs 20) per executed order
    # Since we trade batch qty, the order is ONE order.
    # Total turnover per leg allows us to calculate it.
    brokerage = min(20.0, buy_turnover * 0.0003) + min(20.0, sell_turnover * 0.0003)
    
    # 2. STT: 0.025% on the SELL side only
    stt = sell_turnover * 0.00025
    
    # 3. Exchange tx charge: 0.00325% on both
    exchange_charges = (buy_turnover + sell_turnover) * 0.0000325
    
    # 4. Clearing charges: Rs 0
    
    # 5. GST: 18% on (Brokerage + SEBI + Exchange charges)
    sebi_charges = (buy_turnover + sell_turnover) * 0.000001
    gst = 0.18 * (brokerage + exchange_charges + sebi_charges)
    
    # 6. Stamp Duty: 0.003% on BUY side only
    stamp_duty = buy_turnover * 0.00003
    
    total_charges = brokerage + stt + exchange_charges + sebi_charges + gst + stamp_duty
    return total_charges

def run_backtest():
    filepath = "/Users/adityabansal/Desktop/Arbitrage/BackTesting/Aether_Kite_60D_1min.xlsx"
    print(f"Loading data from {filepath}...")
    df = pd.read_excel(filepath)
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df = df.sort_values('Datetime').reset_index(drop=True)
    
    capital = 1000.0
    initial_capital = capital
    threshold_perc = 0.10  
    
    in_trade = False
    trade_start_time = None
    buy_exchange = None
    sell_exchange = None
    entry_buy_px = 0.0
    entry_sell_px = 0.0
    trade_qty = 0
    
    # Track the delayed execution limit conditions
    pending_entry = None
    
    trade_logs = []
    capital_curve = []
    
    print(f"Starting REALISTIC simulation with ₹{capital} capital, {threshold_perc}% threshold...")
    print("Includes full Intraday STT, Brokerage, GST, Stamp Duty, and 5-second execution delay.")
    
    for idx, row in df.iterrows():
        dt = row['Datetime']
        nse_close = row['NSE_close']
        bse_close = row['BSE_close']
        nse_open = row['NSE_open']
        bse_open = row['BSE_open']
        
        if pd.isna(nse_close) or pd.isna(bse_close) or pd.isna(nse_open) or pd.isna(bse_open):
            capital_curve.append(capital)
            continue
            
        time_of_day = dt.time()
        is_eod = time_of_day.hour == 15 and time_of_day.minute >= 15
        
        # 5s execution delay estimation (Price at Open + 1/12th into the minute)
        # We model this by interpolating between Open and Close.
        nse_10s_price = nse_open + ((nse_close - nse_open) * (5.0 / 60.0))
        bse_10s_price = bse_open + ((bse_close - bse_open) * (5.0 / 60.0))
        
        # PROCESS PENDING ENTRY (from previous minute's close logic)
        if pending_entry and not in_trade:
            # Check if 10s delayed price falls within our Limit Orders
            # If delayed market price moved beyond our Limit, the order won't fill.
            limit_buy = pending_entry['limit_buy']
            limit_sell = pending_entry['limit_sell']
            bx = pending_entry['buy_exchange']
            
            # The delayed available prices
            delayed_cheaper_px = nse_10s_price if bx == 'NSE' else bse_10s_price
            delayed_expensive_px = bse_10s_price if bx == 'NSE' else nse_10s_price
            
            # Since limit buy is ceiling and limit sell is floor, we check if delayed prices still satisfy them
            # Limit Buy condition: Available Market Price <= Our Limit Price
            # Limit Sell condition: Available Market Price >= Our Limit Price
            if delayed_cheaper_px <= limit_buy and delayed_expensive_px >= limit_sell:
                # Order FILLS! At the actual market spread 10 seconds later.
                entry_buy_px = delayed_cheaper_px
                entry_sell_px = delayed_expensive_px
                
                total_exposure_per_share = entry_buy_px + entry_sell_px
                margin_per_share = total_exposure_per_share * 0.20  # 5x MIS leverage
                qty = int(capital // margin_per_share)
                
                if qty > 0:
                    in_trade = True
                    trade_start_time = dt # Filled 10s into this bar
                    buy_exchange = pending_entry['buy_exchange']
                    sell_exchange = pending_entry['sell_exchange']
                    trade_qty = qty
            
            # Regardless of fill or miss, the pending entry is cleared
            pending_entry = None
                
        # PROCESS EXISTING TRADE
        if in_trade:
            current_long_px = nse_close if buy_exchange == 'NSE' else bse_close
            current_short_px = bse_close if sell_exchange == 'BSE' else nse_close
            
            spread_converged = current_long_px >= current_short_px
            
            if spread_converged or is_eod:
                # Execution for Exit (Assuming Market Orders for fast exit, so we get closing price)
                # If we want 10s delay on exit too, we would pend it, but broker EOD is market immediately.
                exit_sell_px = current_long_px
                exit_buy_px = current_short_px
                
                # Gross
                profit_long_leg = (exit_sell_px - entry_buy_px) * trade_qty
                profit_short_leg = (entry_sell_px - exit_buy_px) * trade_qty
                gross_pnl = profit_long_leg + profit_short_leg
                
                # Calculate True Charges
                entry_buy_turnover = entry_buy_px * trade_qty
                entry_sell_turnover = entry_sell_px * trade_qty
                exit_buy_turnover = exit_buy_px * trade_qty
                exit_sell_turnover = exit_sell_px * trade_qty
                
                entry_charges = calculate_charges(entry_buy_turnover, entry_sell_turnover)
                exit_charges = calculate_charges(exit_buy_turnover, exit_sell_turnover)
                total_charges = entry_charges + exit_charges
                
                net_pnl = gross_pnl - total_charges
                capital += net_pnl
                
                trade_logs.append({
                    "Entry Time": trade_start_time,
                    "Exit Time": dt,
                    "Buy Exchange": buy_exchange,
                    "Sell Exchange": sell_exchange,
                    "Quantity": trade_qty,
                    "Entry Buy": round(entry_buy_px, 2),
                    "Entry Sell": round(entry_sell_px, 2),
                    "Exit Sell": round(exit_sell_px, 2),
                    "Exit Buy": round(exit_buy_px, 2),
                    "Gross PnL": round(gross_pnl, 2),
                    "Total Charges": round(total_charges, 2),
                    "Net PnL": round(net_pnl, 2),
                    "Capital After": round(capital, 2),
                    "Exit Reason": "EOD" if is_eod else "Converged"
                })
                in_trade = False
                
        # LOOK FOR NEW ENTRIES
        # If not in trade & no pending entry, check the CLOSE of this minute as our trigger.
        if not in_trade and not pending_entry:
            if is_eod or (time_of_day.hour == 15 and time_of_day.minute > 10):
                capital_curve.append(capital)
                continue
                
            price_diff = abs(nse_close - bse_close)
            avg_price = (nse_close + bse_close) / 2
            threshold = (threshold_perc / 100) * avg_price
            
            if price_diff > threshold:
                buffer = 0.25 * (threshold_perc / 100) * avg_price
                lower_price = min(nse_close, bse_close)
                higher_price = max(nse_close, bse_close)
                
                limit_buy = math.ceil((lower_price + buffer) / 0.05) * 0.05
                limit_sell = math.floor((higher_price - buffer) / 0.05) * 0.05
                
                bx = 'NSE' if nse_close < bse_close else 'BSE'
                sx = 'BSE' if nse_close < bse_close else 'NSE'
                
                # Setup pending entry to be executed on the NEXT minute row with 10s delay logic
                pending_entry = {
                    'buy_exchange': bx,
                    'sell_exchange': sx,
                    'limit_buy': limit_buy,
                    'limit_sell': limit_sell,
                    'signal_time': dt
                }

        capital_curve.append(capital)

    print(f"\n--- RESULTS ---")
    print(f"Final Capital: ₹{round(capital, 2)}")
    print(f"Total Completed Trades: {len(trade_logs)}")
    
    if len(trade_logs) > 0:
        logs_df = pd.DataFrame(trade_logs)
        out_path = "/Users/adityabansal/Desktop/Arbitrage/BackTesting/Backtest_Trades_Return_Delayed.xlsx"
        logs_df.to_excel(out_path, index=False)
        print(f"Realistic trades saved to: {out_path}")
        
if __name__ == "__main__":
    run_backtest()
