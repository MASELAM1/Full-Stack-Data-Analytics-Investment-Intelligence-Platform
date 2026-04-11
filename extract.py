import pandas as pd
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import requests
from sqlalchemy import create_engine, text
import time
from io import StringIO



#Read the downloaded files
listings  = pd.read_csv('listings.csv.gz', compression = 'gzip')
calendar = pd.read_csv('calendar.csv.gz', compression = 'gzip')
reviews = pd.read_csv('reviews.csv.gz', compression = 'gzip')

print(f"Listings: {len(listings)} rows")
print(f"Calendar: {len(calendar)} rows")
print(f"Reviews: {len(reviews)} rows")
print(listings.head())

#Data quality check
def clean_listings(df):
    """Clean and transform listings data"""
    # Remove duplicates
    original_count = len(df)
    df = df.drop_duplicates(subset=['id'])
    print(f"  Removed {original_count - len(df)} duplicates")
    
    # Clean price column (remove $ and commas)
    if 'price' in df.columns:
        df['price'] = df['price'].astype(str).str.replace('$', '', regex=False)
        df['price'] = df['price'].str.replace(',', '', regex=False)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Remove outliers (>99th percentile)
        cap = df['price'].quantile(0.99)
        before_cap = len(df)
        df = df[df['price'] <= cap]
        print(f"  Removed {before_cap - len(df)} price outliers (>R{cap:,.0f})")
    
    # Handle missing values - FIXED TYPO HERE
    if 'review_scores_rating' in df.columns:
        missing_count = df['review_scores_rating'].isna().sum()
        df['review_scores_rating'].fillna(0, inplace=True)
        print(f"  Filled {missing_count} missing review scores")
    
    if 'bedrooms' in df.columns:
        df['bedrooms'].fillna(0, inplace=True)
    
    if 'bathrooms' in df.columns:
        df['bathrooms'].fillna(0, inplace=True)
    
    print(f"  Final listings count: {len(df):,}")
    return df

def clean_calendar(df):
    """Clean calendar data"""
    # Convert date to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Clean price column
    if 'price' in df.columns:
        df['price'] = df['price'].astype(str).str.replace('$', '', regex=False)
        df['price'] = df['price'].str.replace(',', '', regex=False)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Create availability flag
    if 'available' in df.columns:
        df['is_available'] = df['available'] == 't'
    
    print(f"  Calendar rows: {len(df):,}")
    return df

def clean_reviews(df):
    """Clean and transform reviews data"""
    print("  Cleaning reviews data...")
    
    original_count = len(df)
    
    # Remove duplicates
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'])
        print(f"    Removed {original_count - len(df)} duplicates")
    
    # Convert date to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        print(f"    Converted date column to datetime")
    
    # Handle missing reviewer names
    if 'reviewer_name' in df.columns:
        df['reviewer_name'].fillna('Anonymous', inplace=True)
    
    # Handle missing comments
    if 'comments' in df.columns:
        df['comments'].fillna('No comment provided', inplace=True)
    
    # Add review length feature (for analysis)
    if 'comments' in df.columns:
        df['review_length'] = df['comments'].astype(str).str.len()
    
    # Extract year and month for time-based analysis
    if 'date' in df.columns:
        df['review_year'] = df['date'].dt.year
        df['review_month'] = df['date'].dt.month
        df['review_quarter'] = df['date'].dt.quarter
    
    # Calculate reviewer statistics (number of reviews per reviewer)
    if 'reviewer_id' in df.columns:
        reviewer_stats = df.groupby('reviewer_id').size().reset_index(name='reviews_by_reviewer')
        df = df.merge(reviewer_stats, on='reviewer_id', how='left')
    
    print(f"    Final reviews count: {len(df):,}")
    print(f"    Date range: {df['date'].min()} to {df['date'].max()}")
    
    return df

listings_clean = clean_listings(listings)
calendar_clean = clean_calendar(calendar)
reviews_clean = clean_reviews(reviews)

print("\nCalculating occupancy rates..")
#Calculate occupancy from calendar data
occupancy = calendar_clean.groupby('listing_id').agg({
    'is_available': lambda x: (~x).mean() *100 #Occupancy = not available
}).rename(columns= {'is_available': 'occupancy_rate'})

print(f" Calculated occupancy for {len(occupancy):,} listings")

#Merge occupancy into listings
listings_clean = listings_clean.merge(
    occupancy,
    left_on='id',
    right_index=True,
    how='left'
)
listings_clean['occupancy_rate'].fillna(0, inplace=True)
print(f" Average occupancy rate: {listings_clean['occupancy_rate'].mean():.1f}%")

print("\n Calculating Investment Metric..")
#Calculate monthly revenue
listings_clean['estimated_monthly_revenue'] = (
    listings_clean['price'] * (listings_clean['occupancy_rate'] / 100) * 30
)

#Calculate investment score (0 - 100)
def calculate_investment_score(row):
    score = 0
    
    #Price factore (30 points) - sweet spot R800 - R2500
    if 800 <= row['price'] <=2500:
        score += 30
    elif 500 <= row['price'] < 800:
        score +=20
    elif row['price'] > 4000:
        score += 5
    else:
        score += 10
        
    #Occupancy factor (40 points)
    if row['occupancy_rate'] >= 70:
        score += 40
    elif row['occupancy_rate'] >= 50:
        score += 30
    elif row['occupancy_rate'] >= 40:
        score += 15
    else:
        score += 5
        
     # Review score factor (20 points)
    if row['review_scores_rating'] >= 4.8:
        score += 20
    elif row['review_scores_rating'] >= 4.5:
        score += 15
    elif row['review_scores_rating'] >= 4.0:
        score += 10
    else:
        score += 5
        
    # Availability factor (10 points)
    if 100 <= row['availability_365'] <= 250:
        score += 10
    elif row['availability_365'] < 100:
        score += 5
    
    return min(score, 100)

listings_clean['investment_score'] = listings_clean.apply(calculate_investment_score, axis=1)
listings_clean['investment_category'] = pd.cut(
    listings_clean['investment_score'],
    bins=[0, 40, 60, 80, 101],
    labels=['Poor', 'Fair', 'Good', 'Excellent']
)

print(f"  Average investment score: {listings_clean['investment_score'].mean():.1f}")
print(f"  Excellent properties (80+): {len(listings_clean[listings_clean['investment_score'] >= 80]):,}")
print(f"  Good properties (60-79): {len(listings_clean[(listings_clean['investment_score'] >= 60) & (listings_clean['investment_score'] < 80)]):,}")

print("\n Calculating review metrics...")

if reviews_clean is not None and len(reviews_clean) > 0:
    # Calculate reviews per month for each listing
    review_counts = reviews_clean.groupby('listing_id').size().reset_index(name='total_review_count')
    
    # Calculate average rating per listing from reviews (if available)
    if 'review_scores_rating' in reviews_clean.columns:
        avg_rating = reviews_clean.groupby('listing_id')['review_scores_rating'].mean().reset_index()
        avg_rating.columns = ['listing_id', 'avg_review_rating_from_reviews']
        review_counts = review_counts.merge(avg_rating, on='listing_id', how='left')
    
    #Merge with listings
    listings_clean = listings_clean.merge(
        review_counts,
        left_on = 'id',
        right_on = 'listing_id',
        how = 'left'
    )
    listings_clean['total_review_count'].fillna(0, inplace = True)
    
    # Calculate reviews per month (assuming oldest review is at least 1 month old)
    if len(reviews_clean) > 0:
        date_range_days = (reviews_clean['date'].max() - reviews_clean['date'].min()).days
        months_active = max(date_range_days / 30, 1)
        listings_clean['reviews_per_month'] = listings_clean['total_review_count'] / months_active
        listings_clean['reviews_per_month'] = listings_clean['reviews_per_month'].round(2)
    
    print(f"  Added review metrics to {len(listings_clean):,} listings")
    print(f"  Average reviews per listing: {listings_clean['total_review_count'].mean():.1f}")
    
    print("\n Saving cleaned data...")

# Save as Parquet (fast, compressed)
listings_clean.to_parquet('listings_cleaned.parquet', index=False)
calendar_clean.to_parquet('calendar_cleaned.parquet', index=False)
if reviews_clean is not None:
    reviews_clean.to_parquet('reviews_cleaned.parquet', index=False)

print(f" Saved listings_cleaned.parquet ({len(listings_clean):,} rows)")
print(f" Saved calendar_cleaned.parquet ({len(calendar_clean):,} rows)")
if reviews_clean is not None:
    print(f" Saved reviews_cleaned.parquet ({len(reviews_clean):,} rows)")

# Also save as CSV for Power BI (easier to import)
listings_clean.to_csv('listings_cleaned.csv', index=False)
print(f" Saved listings_cleaned.csv for Power BI")

# ============================================
# GENERATE SUMMARY REPORT
# ============================================
print("\n" + "=" * 70)
print(" ETL COMPLETION SUMMARY")
print("=" * 70)

print(f"""
DATA VOLUME:
-----------
Listings:     {len(listings_clean):,} properties
Calendar:     {len(calendar_clean):,} rows
Reviews:      {len(reviews_clean):,} reviews (if available)

MARKET METRICS:
--------------
Average Price:           R{listings_clean['price'].mean():,.0f}
Average Occupancy:       {listings_clean['occupancy_rate'].mean():.1f}%
Average Monthly Revenue: R{listings_clean['estimated_monthly_revenue'].mean():,.0f}
Average Investment Score: {listings_clean['investment_score'].mean():.1f}

INVESTMENT DISTRIBUTION:
-----------------------
Excellent (80-100): {len(listings_clean[listings_clean['investment_score'] >= 80]):,} properties
Good (60-79):       {len(listings_clean[(listings_clean['investment_score'] >= 60) & (listings_clean['investment_score'] < 80)]):,} properties
Fair (40-59):       {len(listings_clean[(listings_clean['investment_score'] >= 40) & (listings_clean['investment_score'] < 60)]):,} properties
Poor (0-39):        {len(listings_clean[listings_clean['investment_score'] < 40]):,} properties

TOP 5 NEIGHBORHOODS BY REVENUE:
""")

# Show top neighborhoods
neighborhood_stats = listings_clean.groupby('neighbourhood_cleansed').agg({
    'estimated_monthly_revenue': 'mean',
    'price': 'mean',
    'occupancy_rate': 'mean'
}).round(2).sort_values('estimated_monthly_revenue', ascending=False).head()

for hood, row in neighborhood_stats.iterrows():
    print(f"  {hood}: R{row['estimated_monthly_revenue']:,.0f}/month (R{row['price']:,.0f}/night, {row['occupancy_rate']:.0f}% occ)")

print("\n" + "=" * 70)
print(" ETL PIPELINE COMPLETED SUCCESSFULLY!")
print("=" * 70)

# Save summary to file
with open('etl_summary.txt', 'w') as f:
    f.write(f"ETL Pipeline completed on {pd.Timestamp.now()}\n")
    f.write(f"Total listings processed: {len(listings_clean):,}\n")
    f.write(f"Average price: R{listings_clean['price'].mean():,.0f}\n")
    f.write(f"Average occupancy: {listings_clean['occupancy_rate'].mean():.1f}%\n")
    f.write(f"Average investment score: {listings_clean['investment_score'].mean():.1f}\n")
    f.write(f"Top neighborhood: {neighborhood_stats.index[0]}\n")

print("\n Full summary saved to etl_summary.txt")

""""
#Databse Connection
engine = create_engine('postgresql://postgres:Dts%40315@localhost:5432/airbnb_investment')

#Load staging tables
listings_clean.to_sql('stg_listings', engine, if_exists='replace', index=False)
calendar_clean.to_sql('stg_calendar', engine, if_exists='replace', index=False)
reviews_clean.to_sql('stg_reviews', engine, if_exists='replace', index=False)
"""
DATABASE_URL = 'postgresql://postgres:Dts%40315@localhost:5432/airbnb_investment'

print("Connection to PostgreSQL...")
engine = create_engine(DATABASE_URL)

#Test connection
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Database connection successful!")
except Exception as e:
    print(f" Connection failed: {e}")
    exit(1)
    
#Load to staging tables

print("\nLoading data to PostgreSQL...")
listings_clean.to_sql('stg_listings', engine, if_exists='replace', index=False)
print(f"✓ Loaded {len(listings_clean):,} rows to stg_listings")

calendar_clean.to_sql('stg_calendar', engine, if_exists='replace', index=False)
print(f"✓ Loaded {len(calendar_clean):,} rows to stg_calendar")

reviews_clean.to_sql('stg_reviews', engine, if_exists='replace', index=False)
print(f"✓ Loaded {len(reviews_clean):,} rows to stg_reviews")

print("\ Data successfully loaded to PostgreSQL!")