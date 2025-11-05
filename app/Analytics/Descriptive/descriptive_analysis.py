# Analytics/Predictive_Modeling/descriptive_analysis.py

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
import numpy as np


current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..')) 

# Add the project root to the system path
if project_root not in sys.path:
    sys.path.append(project_root)

# Import the data loader utility
from data_loader import load_base_sales_data 

# Set plotting style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 100

# --- GLOBAL PLOT SETTINGS ---
# Define a solid color for all non-highlighted bars (Steel Blue)
SOLID_COLOR = '#4682B4'
# Define a highlight color for high-impact months (Crimson Red)
HIGHLIGHT_COLOR = '#DC143C'
# ----------------------------


# Define the output directory relative to where the script is executed
OUTPUT_DIR = 'app/Analytics/'


def generate_eda_plots(base_df):
    """Generates and saves all descriptive plots to the defined OUTPUT_DIR."""
    
    # 1. Ensure the output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Saving plots to: {OUTPUT_DIR}")
    
    df_temp = base_df.reset_index().rename(columns={'date': 'Date'})
    df_temp['DayOfWeek'] = df_temp['Date'].dt.day_name()
    df_temp['Month'] = df_temp['Date'].dt.month
    df_temp['Year'] = df_temp['Date'].dt.year
    df_temp['MonthName'] = df_temp['Date'].dt.strftime('%b')
    
    
    # --- A. Time Series Trend Plot ---
    print("-> 3. Generating Time Series Trend Plot...")
    plt.figure(figsize=(12, 6))
    base_df_daily = base_df.groupby(base_df.index)['gross_revenue'].sum()
    base_df_daily.plot(title='Total Daily Gross Revenue Over Time', linewidth=1.5, color=SOLID_COLOR)
    plt.xlabel('Date')
    plt.ylabel('Gross Revenue (PHP)')
    plt.savefig(os.path.join(OUTPUT_DIR, 'descriptive_trend_plot.png')) 
    plt.close()

    # --- B. Seasonal Decomposition ---
    print("-> 4. Generating Seasonal Decomposition Plot...")
    try:       
        decomposition = seasonal_decompose(base_df_daily, model='additive', period=7)
        
        # Set figure size to a readable, optimal size
        fig = plt.figure(figsize=(14, 20)) 
        
        # Plot the decomposition onto the figure
        fig = decomposition.plot()
        
        # FIX: Get all subplot axes from the figure object (fig.get_axes())
        axes = fig.get_axes()
        
        # Apply date formatting to all subplots to prevent jamming
        for ax in axes:
            # Set major ticks to be at the start of each year
            years = mdates.YearLocator() 
            ax.xaxis.set_major_locator(years)
            
            # Format the major ticks to display only the year
            years_fmt = mdates.DateFormatter('%Y')
            ax.xaxis.set_major_formatter(years_fmt)
            
            # Rotate the dates for better visibility
            plt.sca(ax) 
            plt.xticks(rotation=45, ha='right')

        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'descriptive_decomposition_plot.png')) 
        plt.close(fig) # Close the figure object

        # --- ISOLATED WEEKLY SEASONALITY PLOT (Warnings Fixed) ---
        print("-> 4b. Generating Isolated Weekly Seasonality Plot...")
        
        # Extract the fixed 7-day pattern
        seasonal_factors = decomposition.seasonal[0:7]
        
        # Create a DataFrame for clean plotting
        df_seasonal = pd.DataFrame({
            'DayFactor': seasonal_factors.values,
            'DayOfWeek': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        })
        
        # Set the order explicitly
        weekly_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        df_seasonal['DayOfWeek'] = pd.Categorical(df_seasonal['DayOfWeek'], categories=weekly_order, ordered=True)
        df_seasonal = df_seasonal.sort_values('DayOfWeek')
        
        plt.figure(figsize=(10, 6))
        # FIX: Removed hue and palette, used color=SOLID_COLOR
        sns.barplot(x='DayOfWeek', y='DayFactor', data=df_seasonal, color=SOLID_COLOR, legend=False) 
        plt.axhline(0, color='gray', linewidth=0.8, linestyle='--')
        plt.title('Isolated Weekly Seasonal Factors (7-Day Cycle)')
        plt.xlabel('Day of Week')
        plt.ylabel('Seasonal Factor (PHP deviation from trend)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'descriptive_isolated_weekly_seasonality.png'))
        plt.close()
        
    except Exception as e:
        print(f"Warning: Could not run decomposition. Check data continuity. Error: {e}")

    # --- C. Seasonality Deep Dive ---
    print("-> 5. Generating Seasonality Charts...")
    
    # Weekly Bar Chart (Warnings Fixed)
    weekly_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekly_avg = df_temp.groupby('DayOfWeek')['gross_revenue'].mean().reindex(weekly_order).reset_index()
    
    plt.figure(figsize=(8, 5))
    # FIX: Removed hue and palette, used color=SOLID_COLOR
    sns.barplot(x='DayOfWeek', y='gross_revenue', data=weekly_avg, color=SOLID_COLOR, legend=False)
    plt.title('Average Daily Revenue by Day of Week')
    plt.xlabel('Day of Week')
    plt.ylabel('Avg. Gross Revenue (PHP)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'descriptive_weekly_seasonality.png'))
    plt.close()
    
    # Monthly Bar Chart (Custom Event Coloring)
    monthly_avg = df_temp.groupby('MonthName')['gross_revenue'].mean().reset_index()
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly_avg['MonthName'] = pd.Categorical(monthly_avg['MonthName'], categories=month_order, ordered=True)
    monthly_avg = monthly_avg.sort_values('MonthName')
    
    # Create the custom color list
    highlight_months = ['Oct', 'Nov', 'Dec']
    month_colors = [HIGHLIGHT_COLOR if month in highlight_months else SOLID_COLOR for month in month_order]
    
    plt.figure(figsize=(10, 5))
    # This chart requires hue and palette for multi-color display
    sns.barplot(x='MonthName', y='gross_revenue', hue='MonthName', data=monthly_avg, palette=month_colors, legend=False)
    plt.title('Average Daily Revenue by Month (Event Highlights)')
    plt.xlabel('Month')
    plt.ylabel('Avg. Gross Revenue (PHP)')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'descriptive_monthly_seasonality.png'))
    plt.close()


    # --- D. Platform Comparison ---
    print("-> 6. Generating Platform Comparison Plot...")
    
    # Platform Time Series 
    plt.figure(figsize=(12, 6))
    # This chart requires hue and palette for multi-color line display
    sns.lineplot(data=df_temp, x='Date', y='gross_revenue', hue='platform_name', linewidth=1.5, palette=['#1f77b4', '#ff7f0e']) 
    plt.title('Daily Revenue Trend by Platform')
    plt.xlabel('Date')
    plt.ylabel('Gross Revenue (PHP)')
    plt.legend(title='Platform')
    plt.savefig(os.path.join(OUTPUT_DIR, 'descriptive_platform_trend.png'))
    plt.close()
    
    # Revenue Distribution (Box Plot - Warnings Fixed)
    plt.figure(figsize=(8, 6))
    # FIX: Removed hue and palette, used color=SOLID_COLOR
    sns.boxplot(x='platform_name', y='gross_revenue', data=df_temp, color=SOLID_COLOR, legend=False)
    plt.title('Gross Revenue Distribution by Platform')
    plt.xlabel('Platform')
    plt.ylabel('Gross Revenue (PHP)')
    plt.savefig(os.path.join(OUTPUT_DIR, 'descriptive_platform_boxplot.png'))
    plt.close()


    # --- E. Event Impact Analysis ---
    print("-> 7. Generating Event Impact Analysis...")
    
    # Mega Sale Days (requires hue and palette for two distinct colors)
    event_lift_mega = df_temp.groupby('is_mega_sale_day')['gross_revenue'].mean().reset_index()
    event_lift_mega['is_mega_sale_day'] = event_lift_mega['is_mega_sale_day'].map({0: 'Normal Day', 1: 'Mega Sale Day'})
    
    plt.figure(figsize=(7, 5))
    # Pallete: [Normal Day (SOLID_COLOR), Mega Sale Day (HIGHLIGHT_COLOR)]
    sns.barplot(x='is_mega_sale_day', y='gross_revenue', hue='is_mega_sale_day', 
                data=event_lift_mega, palette=[SOLID_COLOR, HIGHLIGHT_COLOR], legend=False)
    plt.title('Average Revenue Lift on Mega Sale Days')
    plt.xlabel('Day Type')
    plt.ylabel('Avg. Gross Revenue (PHP)')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'descriptive_event_impact_mega.png'))
    plt.close()
    
    # Payday (requires hue and palette for two distinct colors)
    event_lift_payday = df_temp.groupby('is_payday')['gross_revenue'].mean().reset_index()
    event_lift_payday['is_payday'] = event_lift_payday['is_payday'].map({0: 'Normal Day', 1: 'Pay Day'})
    
    plt.figure(figsize=(7, 5))
    # Pallete: [Normal Day (SOLID_COLOR), Pay Day (Custom Green)]
    sns.barplot(x='is_payday', y='gross_revenue', hue='is_payday', 
                data=event_lift_payday, palette=[SOLID_COLOR, '#3CB371'], legend=False)
    plt.title('Average Revenue Lift on Pay Days')
    plt.xlabel('Day Type')
    plt.ylabel('Avg. Gross Revenue (PHP)')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'descriptive_event_impact_payday.png'))
    plt.close()

def run_descriptive_analysis():
    print("1. Starting Descriptive Analysis...")
    base_df = load_base_sales_data()
    
    if base_df.empty:
        print("Analysis failed: No data loaded from Supabase.")
        return

    # ... (Executive Summary Code) ...
    print("\n2. Generating Executive Summary Metrics...")
    
    total_revenue = base_df['gross_revenue'].sum()
    avg_daily_revenue = base_df['gross_revenue'].mean()
    start_date = base_df.index.min().date()
    end_date = base_df.index.max().date()
    total_days = (end_date - start_date).days
    
    print("\n--- Executive Summary ---")
    print(f"Total Revenue (Historical): PHP {total_revenue:,.2f}")
    print(f"Average Daily Revenue: PHP {avg_daily_revenue:,.2f}")
    print(f"Total Period Analyzed: {total_days} days ({start_date} to {end_date})")
    print("-" * 40)
    
    # --- Generate all plots ---
    generate_eda_plots(base_df)

    print(f"\nDescriptive Analysis Complete. All charts saved as PNG files in the {OUTPUT_DIR} directory.")


if __name__ == '__main__':
    run_descriptive_analysis()