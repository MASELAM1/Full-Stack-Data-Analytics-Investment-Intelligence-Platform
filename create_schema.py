"""
create_schema.py - Create star schema for Airbnb investment analytics
Run this AFTER your data is loaded to stg_ tables
"""
from sqlalchemy import create_engine, text
import pandas as pd

# Database connection
DATABASE_URL = 'postgresql://postgres:Dts%40315@localhost:5432/airbnb_investment'
engine = create_engine(DATABASE_URL)

print("=" * 70)
print("CREATING STAR SCHEMA FOR AIRBNB ANALYTICS")
print("=" * 70)

# ============================================
#  Create Schemas
# ============================================
print("\n Creating schemas...")

with engine.connect() as conn:
    # Create analytics schema
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
    print("Created schema: analytics")
    
    # Create warehouse schema (for transformed data)
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse"))
    print(" Created schema: warehouse")
    
    conn.commit()

# ============================================
#  Create Dimension Tables
# ============================================
print("\n Creating dimension tables...")

dimension_sql = """
-- Dimension: Neighborhood
CREATE TABLE IF NOT EXISTS warehouse.dim_neighbourhood (
    neighbourhood_id SERIAL PRIMARY KEY,
    neighbourhood_name VARCHAR(200) UNIQUE NOT NULL,
    area VARCHAR(100),
    price_multiplier DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Property Type
CREATE TABLE IF NOT EXISTS warehouse.dim_property_type (
    property_type_id SERIAL PRIMARY KEY,
    room_type VARCHAR(50) UNIQUE NOT NULL,
    category VARCHAR(50),
    typical_guests INTEGER
);

-- Dimension: Date (for time-based analysis)
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id DATE PRIMARY KEY,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Dimension: Investment Grade
CREATE TABLE IF NOT EXISTS warehouse.dim_investment_grade (
    grade_id SERIAL PRIMARY KEY,
    grade_name VARCHAR(20) UNIQUE NOT NULL,
    score_min INTEGER,
    score_max INTEGER,
    recommendation VARCHAR(50)
);
"""

with engine.connect() as conn:
    for statement in dimension_sql.split(';'):
        if statement.strip():
            conn.execute(text(statement))
            conn.commit()
    print(" Created dimension tables")

# ============================================
# Populate Dimension Tables
# ============================================
print("\n Populating dimension tables...")

# Populate neighborhood dimension from staging data
df_neighborhoods = pd.read_sql("""
    SELECT DISTINCT neighbourhood_cleansed as neighbourhood_name
    FROM stg_listings
    WHERE neighbourhood_cleansed IS NOT NULL
""", engine)

df_neighborhoods.to_sql('dim_neighbourhood', engine, schema='warehouse', 
                         if_exists='append', index=False)
print(f"  Loaded {len(df_neighborhoods)} neighborhoods")

# Populate property type dimension
df_room_types = pd.read_sql("""
    SELECT DISTINCT 
        room_type,
        CASE 
            WHEN room_type ILIKE '%entire%' THEN 'Entire Home'
            WHEN room_type ILIKE '%private%' THEN 'Private Room'
            WHEN room_type ILIKE '%shared%' THEN 'Shared Room'
            ELSE 'Other'
        END as category
    FROM stg_listings
    WHERE room_type IS NOT NULL
""", engine)

df_room_types.to_sql('dim_property_type', engine, schema='warehouse', 
                      if_exists='append', index=False)
print(f"  Loaded {len(df_room_types)} property types")

# Populate investment grade dimension
investment_grades = [
    ('Excellent', 80, 100, 'Strong Buy - High ROI potential'),
    ('Good', 60, 79, 'Buy - Solid investment'),
    ('Fair', 40, 59, 'Consider - Review details'),
    ('Poor', 0, 39, 'Avoid - Low potential')
]

df_grades = pd.DataFrame(investment_grades, 
                         columns=['grade_name', 'score_min', 'score_max', 'recommendation'])
df_grades.to_sql('dim_investment_grade', engine, schema='warehouse', 
                  if_exists='append', index=False)
print(f"  Loaded {len(df_grades)} investment grades")

# ============================================
# Create Fact Table
# ============================================
print("\n Creating fact table...")

fact_table_sql = """
CREATE TABLE IF NOT EXISTS warehouse.fact_listings (
    listing_id INTEGER PRIMARY KEY,
    neighbourhood_id INTEGER REFERENCES warehouse.dim_neighbourhood(neighbourhood_id),
    property_type_id INTEGER REFERENCES warehouse.dim_property_type(property_type_id),
    grade_id INTEGER REFERENCES warehouse.dim_investment_grade(grade_id),
    
    -- Property details
    name TEXT,
    host_id INTEGER,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    accommodates INTEGER,
    bedrooms INTEGER,
    bathrooms DECIMAL(3,1),
    
    -- Pricing
    price DECIMAL(10,2),
    minimum_nights INTEGER,
    maximum_nights INTEGER,
    
    -- Performance metrics
    number_of_reviews INTEGER,
    review_scores_rating DECIMAL(3,2),
    availability_365 INTEGER,
    occupancy_rate DECIMAL(5,2),
    
    -- Investment metrics
    estimated_monthly_revenue DECIMAL(12,2),
    estimated_annual_revenue DECIMAL(12,2),
    investment_score DECIMAL(5,2),
    
    -- Review metrics
    total_review_count INTEGER,
    reviews_per_month DECIMAL(5,2),
    
    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_neighbourhood ON warehouse.fact_listings(neighbourhood_id);
CREATE INDEX IF NOT EXISTS idx_fact_investment_score ON warehouse.fact_listings(investment_score DESC);
CREATE INDEX IF NOT EXISTS idx_fact_price ON warehouse.fact_listings(price);
CREATE INDEX IF NOT EXISTS idx_fact_occupancy ON warehouse.fact_listings(occupancy_rate);
"""

with engine.connect() as conn:
    for statement in fact_table_sql.split(';'):
        if statement.strip():
            conn.execute(text(statement))
            conn.commit()
    print("✓ Created fact table")

# ============================================
# Load Fact Table from Staging
# ============================================
print("\n Loading fact table...")

# Get dimension mappings
neighborhood_map = pd.read_sql(
    "SELECT neighbourhood_id, neighbourhood_name FROM warehouse.dim_neighbourhood", 
    engine
)
property_type_map = pd.read_sql(
    "SELECT property_type_id, room_type FROM warehouse.dim_property_type", 
    engine
)

# Load and transform staging data
df_fact = pd.read_sql("""
    SELECT 
        id as listing_id,
        name,
        host_id,
        latitude,
        longitude,
        accommodates,
        bedrooms,
        bathrooms,
        price,
        minimum_nights,
        maximum_nights,
        number_of_reviews,
        review_scores_rating,
        availability_365,
        occupancy_rate,
        estimated_monthly_revenue,
        estimated_monthly_revenue * 12 as estimated_annual_revenue,
        investment_score,
        total_review_count,
        reviews_per_month,
        neighbourhood_cleansed,
        room_type
    FROM stg_listings
""", engine)

# Add dimension IDs
df_fact = df_fact.merge(neighborhood_map, 
                         left_on='neighbourhood_cleansed', 
                         right_on='neighbourhood_name', 
                         how='left')

df_fact = df_fact.merge(property_type_map,
                         left_on='room_type',
                         right_on='room_type',
                         how='left')

# Add grade ID based on investment score
df_fact['grade_id'] = 4  # Default to Poor
df_fact.loc[df_fact['investment_score'] >= 80, 'grade_id'] = 1
df_fact.loc[(df_fact['investment_score'] >= 60) & (df_fact['investment_score'] < 80), 'grade_id'] = 2
df_fact.loc[(df_fact['investment_score'] >= 40) & (df_fact['investment_score'] < 60), 'grade_id'] = 3

# Select final columns
fact_columns = ['listing_id', 'neighbourhood_id', 'property_type_id', 'grade_id',
                'name', 'host_id', 'latitude', 'longitude', 'accommodates',
                'bedrooms', 'bathrooms', 'price', 'minimum_nights', 'maximum_nights',
                'number_of_reviews', 'review_scores_rating', 'availability_365',
                'occupancy_rate', 'estimated_monthly_revenue', 'estimated_annual_revenue',
                'investment_score', 'total_review_count', 'reviews_per_month']

df_fact_final = df_fact[fact_columns]

# Load to fact table
df_fact_final.to_sql('fact_listings', engine, schema='warehouse', 
                      if_exists='replace', index=False)
print(f"  ✓ Loaded {len(df_fact_final):,} rows to fact table")

# ============================================
# Create Analytical Views
# ============================================
print("\n Creating analytical views...")

views_sql = """
-- View 1: Neighborhood Performance
CREATE OR REPLACE VIEW analytics.vw_neighborhood_performance AS
SELECT 
    n.neighbourhood_name,
    COUNT(f.listing_id) as total_listings,
    ROUND(AVG(f.price)::numeric, 0) as avg_daily_rate,
    ROUND(AVG(f.occupancy_rate)::numeric, 1) as avg_occupancy,
    ROUND(AVG(f.estimated_monthly_revenue)::numeric, 0) as avg_monthly_revenue,
    ROUND(AVG(f.investment_score)::numeric, 1) as avg_investment_score,
    COUNT(CASE WHEN f.investment_score >= 70 THEN 1 END) as high_potential
FROM warehouse.fact_listings f
JOIN warehouse.dim_neighbourhood n ON f.neighbourhood_id = n.neighbourhood_id
GROUP BY n.neighbourhood_name
ORDER BY avg_monthly_revenue DESC;

-- View 2: Top Investment Opportunities
CREATE OR REPLACE VIEW analytics.vw_top_investments AS
SELECT 
    f.listing_id,
    f.name,
    n.neighbourhood_name,
    pt.room_type,
    f.price,
    f.occupancy_rate,
    f.review_scores_rating,
    f.investment_score,
    g.grade_name,
    g.recommendation,
    f.estimated_monthly_revenue,
    ROUND((f.estimated_monthly_revenue * 12 / NULLIF(f.price * 365, 0))::numeric * 100, 1) as projected_roi_percent
FROM warehouse.fact_listings f
JOIN warehouse.dim_neighbourhood n ON f.neighbourhood_id = n.neighbourhood_id
JOIN warehouse.dim_property_type pt ON f.property_type_id = pt.property_type_id
JOIN warehouse.dim_investment_grade g ON f.grade_id = g.grade_id
WHERE f.investment_score >= 60
ORDER BY f.investment_score DESC;

-- View 3: Market Summary
CREATE OR REPLACE VIEW analytics.vw_market_summary AS
SELECT 
    'Total Listings' as metric, COUNT(*)::text as value FROM warehouse.fact_listings
UNION ALL
SELECT 'Avg Price', ROUND(AVG(price)::numeric, 0)::text FROM warehouse.fact_listings
UNION ALL
SELECT 'Avg Occupancy', ROUND(AVG(occupancy_rate)::numeric, 1)::text || '%' FROM warehouse.fact_listings
UNION ALL
SELECT 'Avg Investment Score', ROUND(AVG(investment_score)::numeric, 1)::text FROM warehouse.fact_listings
UNION ALL
SELECT 'High Potential', COUNT(CASE WHEN investment_score >= 70 THEN 1 END)::text FROM warehouse.fact_listings;

-- View 4: Price Segment Analysis
CREATE OR REPLACE VIEW analytics.vw_price_segments AS
SELECT 
    CASE 
        WHEN f.price < 1000 THEN 'Budget (<R1000)'
        WHEN f.price BETWEEN 1000 AND 2000 THEN 'Mid-Range (R1000-2000)'
        WHEN f.price BETWEEN 2000 AND 3500 THEN 'Premium (R2000-3500)'
        ELSE 'Luxury (>R3500)'
    END as price_segment,
    COUNT(*) as total_listings,
    ROUND(AVG(f.occupancy_rate)::numeric, 1) as avg_occupancy,
    ROUND(AVG(f.investment_score)::numeric, 1) as avg_score,
    ROUND(AVG(f.estimated_monthly_revenue)::numeric, 0) as avg_revenue
FROM warehouse.fact_listings f
GROUP BY price_segment
ORDER BY avg_revenue DESC;
"""

with engine.connect() as conn:
    # Create analytics schema if not exists
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
    
    for statement in views_sql.split(';'):
        if statement.strip():
            conn.execute(text(statement))
            conn.commit()
    print(" Created analytical views")

# ============================================
# Verification
# ============================================
print("\n Step 7: Verification...")

with engine.connect() as conn:
    # Check row counts
    result = conn.execute(text("""
        SELECT 
            (SELECT COUNT(*) FROM warehouse.fact_listings) as fact_rows,
            (SELECT COUNT(*) FROM warehouse.dim_neighbourhood) as dim_neighborhoods,
            (SELECT COUNT(*) FROM warehouse.dim_property_type) as dim_property_types,
            (SELECT COUNT(*) FROM warehouse.dim_investment_grade) as dim_grades
    """))
    row = result.fetchone()
    
    print(f"\n{'='*50}")
    print("SCHEMA CREATION COMPLETE!")
    print(f"{'='*50}")
    print(f" Fact Table: {row[0]:,} rows")
    print(f" Neighborhoods: {row[1]} distinct areas")
    print(f" Property Types: {row[2]}")
    print(f" Investment Grades: {row[3]}")

print("\n" + "=" * 70)
print(" STAR SCHEMA SUCCESSFULLY CREATED!")
print("=" * 70)
print("\n Schemas created:")
print("  - warehouse: Dimension and fact tables")
print("  - analytics: Pre-built analytical views")
print("\n Ready for Power BI. Connect to:")
print("  - analytics.vw_neighborhood_performance")
print("  - analytics.vw_top_investments")
print("  - analytics.vw_market_summary")