import os
import sys
import datetime as dt
import pandas as pd
from kiteconnect import KiteConnect
import warnings
from urllib.parse import urlparse, parse_qs

# Suppress warnings
warnings.filterwarnings('ignore')

API_KEY = "b5012qtc6pcpbzhb"
API_SECRET = "e6b8ivxfk760md5qtgt8ms3xmc20wves"

def get_instrument_token(kite, symbol, exchange):
    try:
        instruments = kite.instruments(exchange)
        for ins in instruments:
            if ins['tradingsymbol'] == symbol:
                return ins['instrument_token']
    except Exception as e:
        print(f"Error fetching instruments from {exchange}: {e}")
    return None

def fetch_data(kite, instrument_token, start_date, end_date):
    """Fetch 1m data in chunks of 30 days from Kite Historical API."""
    dfs = []
    curr_start = start_date
    while curr_start < end_date:
        # Fetch up to 30 days per request to be safe with Kite limits
        curr_end = min(curr_start + dt.timedelta(days=30), end_date)
        try:
            records = kite.historical_data(
                instrument_token=instrument_token,
                from_date=curr_start.strftime("%Y-%m-%d"),
                to_date=curr_end.strftime("%Y-%m-%d"),
                interval="minute",
                continuous=False,
                oi=False
            )
            if records:
                df = pd.DataFrame(records)
                dfs.append(df)
        except Exception as e:
            print(f"Failed to fetch data between {curr_start} and {curr_end}: {e}")
        curr_start = curr_end + dt.timedelta(days=1)
        
    if dfs:
        ans = pd.concat(dfs)
        ans.set_index('date', inplace=True)
        ans.index = ans.index.tz_localize(None)
        ans = ans[~ans.index.duplicated(keep='first')]
        return ans
    return pd.DataFrame()

def main():
    print("Initialising KiteConnect...")
    kite = KiteConnect(api_key=API_KEY)
    
    access_token = None
    
    # Check if passed as arg
    if len(sys.argv) > 1:
        access_token = sys.argv[1]
        kite.set_access_token(access_token)
        try:
            profile = kite.profile()
            print(f"Logged in successfully as {profile.get('user_name', 'User')}")
        except Exception:
            print("Provided access token is invalid or expired. We will generate a new one.")
            access_token = None

    if not access_token:
        print("\n--- KITE LOGIN REQUIRED ---")
        login_url = kite.login_url()
        print(f"1. Please open this URL in your web browser: \n\n{login_url}\n")
        print("2. Log into your Zerodha Kite account.")
        print("3. After successful login, the browser will redirect you to a new page (even if it says 'Site cannot be reached', that is COMPLETELY FINE).")
        print("4. Copy the ENTIRE URL from the browser's address bar and paste it below:\n")
        
        redirected_url = input("Paste the URL here: ").strip()
        
        try:
            parsed_url = urlparse(redirected_url)
            query_params = parse_qs(parsed_url.query)
            
            if 'request_token' not in query_params:
                print("Error: Could not find 'request_token' in the pasted URL. Please make sure you copied the entire URL correctly.")
                sys.exit(1)
                
            request_token = query_params['request_token'][0]
            print(f"Extracted request_token: {request_token}")
            
            print("Generating session...")
            data = kite.generate_session(request_token, api_secret=API_SECRET)
            access_token = data['access_token']
            kite.set_access_token(access_token)
            
            print(f"\n=> SUCCESS! Your session is active. (Feel free to save your new Access Token: {access_token})\n")
            
        except Exception as e:
            print(f"Failed to generate session: {e}")
            sys.exit(1)

    print("Fetching Instrument Tokens for AETHER...")
    nse_token = get_instrument_token(kite, "AETHER", "NSE")
    bse_token = get_instrument_token(kite, "AETHER", "BSE")
    
    if not nse_token or not bse_token:
        print(f"Failed to fetch matching tokens! NSE Token: {nse_token}, BSE Token: {bse_token}")
        sys.exit(1)
        
    end_date = dt.datetime.today()
    start_date = end_date - dt.timedelta(days=60)
    
    print(f"Fetching 1-minute historical data from {start_date.date()} to {end_date.date()} (~60 days) ...")
    print("Fetching NSE data...")
    nse_df = fetch_data(kite, nse_token, start_date, end_date)
    
    print("Fetching BSE data...")
    bse_df = fetch_data(kite, bse_token, start_date, end_date)
    
    if nse_df.empty and bse_df.empty:
        print("\n[!] No data was returned by Kite.")
        print("Please verify that your Zerodha Kite API Key has the 'Historical Data' add-on subscription active (it is an extra ₹2000/month above the base API subscription).")
        sys.exit(1)
        
    if not nse_df.empty:
        nse_df = nse_df.add_prefix("NSE_")
    if not bse_df.empty:
        bse_df = bse_df.add_prefix("BSE_")
        
    print("Aligning and Merging NSE and BSE data...")
    if not nse_df.empty and not bse_df.empty:
        base_index = nse_df.index.union(bse_df.index)
    elif not nse_df.empty:
        base_index = nse_df.index
    else:
        base_index = bse_df.index
        
    base_df = pd.DataFrame(index=base_index)
    base_df.index.name = "Datetime"
    
    if not nse_df.empty:
        base_df = base_df.join(nse_df, how='left')
    if not bse_df.empty:
        base_df = base_df.join(bse_df, how='left')
        
    out_dir = "/Users/adityabansal/Desktop/Arbitrage/BackTesting"
    os.makedirs(out_dir, exist_ok=True)
    excel_path = os.path.join(out_dir, "Aether_Kite_60D_1min.xlsx")
    
    print("Saving to Excel...")
    base_df.to_excel(excel_path)
    
    try:
        import openpyxl
        wb = openpyxl.load_workbook(excel_path)
        ws = wb.active
        ws.column_dimensions["A"].width = 20
        wb.save(excel_path)
    except:
        pass
        
    print(f"Successfully generated file: {excel_path}")

if __name__ == "__main__":
    main()
