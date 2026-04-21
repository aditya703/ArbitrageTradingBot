import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    filepath = "/Users/adityabansal/Desktop/Arbitrage/BackTesting/Backtest_Final.xlsx"
    print(f"Loading data from {filepath}...")
    df = pd.read_excel(filepath)
    
    # Set the delay column as the index
    df.set_index("Delay (secs)", inplace=True)
    
    # Transpose so that Threshold Percentages are on the Y-axis (rows)
    # and Execution Delay is on the X-axis (columns)
    heatmap_data = df.T
    
    # Sort index so that the lowest percentage is at the bottom, highest at the top
    # The columns are formatted as strings like '0.08%', so we can sort them
    # by stripping the '%' and converting to float
    heatmap_data.index = heatmap_data.index.astype(str)
    heatmap_data = heatmap_data.iloc[::-1]

    plt.figure(figsize=(12, 8))
    
    # Create the heatmap
    # annot=True puts the numbers in the cells. 
    # fmt=".0f" rounds to whole numbers for readability.
    # cmap="RdYlGn" uses a Red-Yellow-Green colormap (Red=Loss/Low, Green=Profit/High)
    ax = sns.heatmap(heatmap_data, annot=True, fmt=".0f", cmap="RdYlGn", 
                     linewidths=0.5, cbar_kws={'label': 'Final Capital (₹)'}, 
                     center=1000.0) # Break-even is centered
    
    plt.title("Arbitrage Viability: Execution Delay vs. Selection Threshold\n(Base Capital ₹1000)", fontsize=16, pad=15)
    plt.xlabel("Execution Delay (seconds)", fontsize=12, labelpad=10)
    plt.ylabel("Threshold Spread (%)", fontsize=12, labelpad=10)
    
    plt.tight_layout()
    
    out_path = "/Users/adityabansal/Desktop/Arbitrage/BackTesting/Backtest_Graph.png"
    plt.savefig(out_path, dpi=300)
    print(f"Successfully generated graph at: {out_path}")

if __name__ == "__main__":
    main()
