import pandas as pd
import numpy as np
import math
import os

def calculate_charges(buy_turnover, sell_turnover):
    brokerage = min(20.0, buy_turnover * 0.0003) + min(20.0, sell_turnover * 0.0003)
    stt = sell_turnover * 0.00025
    exchange_charges = (buy_turnover + sell_turnover) * 0.0000325
    sebi_charges = (buy_turnover + sell_turnover) * 0.000001
    gst = 0.18 * (brokerage + exchange_charges + sebi_charges)
    stamp_duty = buy_turnover * 0.00003
    return brokerage + stt + exchange_charges + sebi_charges + gst + stamp_duty

def simulate(df, delay_seconds, threshold_perc, initial_capital=1000.0):
    capital = initial_capital
    in_trade = False
    buy_exchange = None
    sell_exchange = None
    entry_buy_px = 0.0
    entry_sell_px = 0.0
    trade_qty = 0
    pending_entry = None

    for idx, row in df.iterrows():
        dt = row['Datetime']
        nse_close = row['NSE_close']
        bse_close = row['BSE_close']
        nse_open = row['NSE_open']
        bse_open = row['BSE_open']
        
        if pd.isna(nse_close) or pd.isna(bse_close) or pd.isna(nse_open) or pd.isna(bse_open):
            continue
            
        time_of_day = dt.time()
        is_eod = time_of_day.hour == 15 and time_of_day.minute >= 15
        
        nse_delayed_px = nse_open + ((nse_close - nse_open) * (delay_seconds / 60.0))
        bse_delayed_px = bse_open + ((bse_close - bse_open) * (delay_seconds / 60.0))
        
        if pending_entry and not in_trade:
            limit_buy = pending_entry['limit_buy']
            limit_sell = pending_entry['limit_sell']
            bx = pending_entry['buy_exchange']
            
            delayed_cheaper_px = nse_delayed_px if bx == 'NSE' else bse_delayed_px
            delayed_expensive_px = bse_delayed_px if bx == 'NSE' else nse_delayed_px
            
            if delayed_cheaper_px <= limit_buy and delayed_expensive_px >= limit_sell:
                entry_buy_px = delayed_cheaper_px
                entry_sell_px = delayed_expensive_px
                
                margin_per_share = (entry_buy_px + entry_sell_px) * 0.20
                qty = int(capital // margin_per_share)
                
                if qty > 0:
                    in_trade = True
                    buy_exchange = pending_entry['buy_exchange']
                    sell_exchange = pending_entry['sell_exchange']
                    trade_qty = qty
            
            pending_entry = None
                
        if in_trade:
            current_long_px = nse_close if buy_exchange == 'NSE' else bse_close
            current_short_px = bse_close if sell_exchange == 'BSE' else nse_close
            
            if current_long_px >= current_short_px or is_eod:
                exit_sell_px = current_long_px
                exit_buy_px = current_short_px
                
                profit_long_leg = (exit_sell_px - entry_buy_px) * trade_qty
                profit_short_leg = (entry_sell_px - exit_buy_px) * trade_qty
                gross_pnl = profit_long_leg + profit_short_leg
                
                entry_charges = calculate_charges(entry_buy_px * trade_qty, entry_sell_px * trade_qty)
                exit_charges = calculate_charges(exit_buy_px * trade_qty, exit_sell_px * trade_qty)
                
                net_pnl = gross_pnl - (entry_charges + exit_charges)
                capital += net_pnl
                
                in_trade = False
                
        if not in_trade and not pending_entry:
            if is_eod or (time_of_day.hour == 15 and time_of_day.minute > 10):
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
                
                pending_entry = {
                    'buy_exchange': bx,
                    'sell_exchange': sx,
                    'limit_buy': limit_buy,
                    'limit_sell': limit_sell
                }

    return capital

def main():
    filepath = "/Users/adityabansal/Desktop/Arbitrage/BackTesting/Aether_Kite_60D_1min.xlsx"
    print("Loading historical data...")
    df = pd.read_excel(filepath)
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df = df.sort_values('Datetime').reset_index(drop=True)
    
    # We will test delay from 1s to 10s
    delays = list(range(1, 11))
    
    # From 0.08% up to 0.30% in 0.01% increments to generate a comprehensive viability map
    # 0.30 - 0.08 = 0.22 / 0.01 = 22 increments -> 23 total values
    thresholds_display = [round(0.08 + (i * 0.01), 3) for i in range(23)] 
    
    results = {
        "Delay (secs)": delays
    }
    
    print("Running parameter sweep... This may take a moment.")
    
    # For each threshold column
    for t_val in thresholds_display:
        col_name = f"{t_val}%"
        results[col_name] = []
        for d_sec in delays:
            # t_val is e.g. 0.08. Passed to simulate which does / 100
            final_cap = simulate(df, d_sec, t_val)
            results[col_name].append(round(final_cap, 2))
            
    # Output to DataFrame
    final_df = pd.DataFrame(results)
    
    out_path = "/Users/adityabansal/Desktop/Arbitrage/BackTesting/Backtest_Final.xlsx"
    final_df.to_excel(out_path, index=False)
    
    print(final_df.to_string(index=False))
    print(f"\nSaved matrix to: {out_path}")

if __name__ == "__main__":
    main()
