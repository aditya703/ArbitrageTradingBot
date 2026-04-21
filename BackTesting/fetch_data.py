import yfinance as yf
import pandas as pd
import datetime as dt
import os
import pytz

def generate_trading_minutes(start_date, end_date):
    """Generate 1-min index for Indian trading hours (mon-fri, 9:15-15:30)"""
    dates = pd.date_range(start_date, end_date, freq='B')
    all_times = []
    for d in dates:
        # 9:15 to 15:30
        times = pd.date_range(
            dt.datetime.combine(d, dt.time(9, 15)), 
            dt.datetime.combine(d, dt.time(15, 30)), 
            freq='1min'
        )
        all_times.extend(times)
        
    idx = pd.DatetimeIndex(all_times)
    idx = idx.tz_localize('Asia/Kolkata', ambiguous='NaT', nonexistent='NaT')
    
    # Drop NaT due to daylight saving/ambiguous times if any
    idx = idx.dropna()
    df = pd.DataFrame(index=idx)
    df.index.name = "Datetime"
    return df

def fetch_data(symbol, start_date, end_date):
    """Fetch 1m data in chunks of 7 days."""
    dfs = []
    curr_start = start_date
    tz = pytz.timezone('Asia/Kolkata')
    
    while curr_start < end_date:
        curr_end = min(curr_start + dt.timedelta(days=7), end_date)
        try:
            df = yf.download(
                symbol, 
                start=curr_start.strftime("%Y-%m-%d"), 
                end=curr_end.strftime("%Y-%m-%d"), 
                interval="1m", 
                progress=False
            )
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            pass
        curr_start = curr_end
    
    if dfs:
        ans = pd.concat(dfs)
        # Handle duplicated index
        ans = ans[~ans.index.duplicated(keep='first')]
        return ans
    return pd.DataFrame()

def main():
    end_date = dt.datetime.today().date()
    start_date = end_date - dt.timedelta(days=180)
    
    # Base timeframe
    base_df = generate_trading_minutes(start_date, end_date)
    
    print("Fetching NSE data for AETHER.NS...")
    nse_df = fetch_data("AETHER.NS", start_date, end_date)
    
    print("Fetching BSE data for AETHER.BO...")
    bse_df = fetch_data("AETHER.BO", start_date, end_date)
    
    # Flatten yfinance MultiIndex output
    if not nse_df.empty and isinstance(nse_df.columns, pd.MultiIndex):
        nse_df.columns = [col[0] for col in nse_df.columns]
    if not bse_df.empty and isinstance(bse_df.columns, pd.MultiIndex):
        bse_df.columns = [col[0] for col in bse_df.columns]
        
    # Prefix columns
    if not nse_df.empty:
        nse_df = nse_df.add_prefix("NSE_")
    if not bse_df.empty:
        bse_df = bse_df.add_prefix("BSE_")
        
    # Merge NSE
    print("Merging data...")
    if not nse_df.empty:
        if nse_df.index.tz is None:
            nse_df.index = nse_df.index.tz_localize('Asia/Kolkata')
        else:
            nse_df.index = nse_df.index.tz_convert('Asia/Kolkata')
        base_df = base_df.join(nse_df, how='left')
    else:
        for col in ["NSE_Open", "NSE_High", "NSE_Low", "NSE_Close", "NSE_Volume"]:
            base_df[col] = float('nan')
            
    # Merge BSE
    if not bse_df.empty:
        if bse_df.index.tz is None:
            bse_df.index = bse_df.index.tz_localize('Asia/Kolkata')
        else:
            bse_df.index = bse_df.index.tz_convert('Asia/Kolkata')
        base_df = base_df.join(bse_df, how='left')
    else:
        for col in ["BSE_Open", "BSE_High", "BSE_Low", "BSE_Close", "BSE_Volume"]:
            base_df[col] = float('nan')
            
    print(f"Data shapes - Base: {base_df.shape}, NSE: {nse_df.shape if not nse_df.empty else (0,0)}, BSE: {bse_df.shape if not bse_df.empty else (0,0)}")
            
    print("Saving to Excel...")
    out_dir = "/Users/adityabansal/Desktop/Arbitrage/BackTesting"
    os.makedirs(out_dir, exist_ok=True)
    base_df.index = base_df.index.tz_localize(None) 
    
    excel_path = os.path.join(out_dir, "Aether_1min_6M.xlsx")
    base_df.to_excel(excel_path)
    print(f"Successfully saved to {excel_path}")

if __name__ == "__main__":
    main()
