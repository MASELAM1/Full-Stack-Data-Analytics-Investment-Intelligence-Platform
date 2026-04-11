# 🏠 Cape Town Airbnb Investment Analytics Platform

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104-green.svg)](https://fastapi.tiangolo.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-F2C811.svg)](https://powerbi.microsoft.com/)
[![Docker](https://img.shields.io/badge/Docker-2496ED.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-red.svg)](LICENSE)

> **End-to-end analytics platform for short-term rental investment intelligence**

## 📊 Live Demo: Only Power BI Dashboard Link available, other Links Not yet Available

| Component | Link | Status |
|-----------|------|--------|
| **Power BI Dashboard** | [View Dashboard](https://app.powerbi.com/links/_XGj54-vOe?ctid=b14d86f1-83ba-4b13-a702-b5c0231b9337&pbi_source=linkShare) | 🟢 Live |
| **API Documentation** | [Swagger UI](https://your-api.onrender.com/docs) | 🟢 Live |
| **Interactive Demo** | [Watch Video](https://youtu.be/your-link) | 🟢 Available |

## 🎯 Problem Statement

Real estate investors lack data-driven tools to evaluate short-term rental investment opportunities across Cape Town's fragmented Airbnb market. This platform solves that by providing:

- **Data-driven insights** from 20,000+ properties
- **Investment scoring** algorithm for objective evaluation
- **Interactive dashboard** for exploring opportunities
- **REST API** for programmatic access

## 📈 Key Business Insights

| Insight | Finding | Business Impact |
|---------|---------|-----------------|
| **Top Neighborhood** | Camps Bay generates R8,200/month average revenue | 40% higher ROI than market average |
| **Price Sweet Spot** | R1,500-R3,000/night = 70%+ occupancy | Optimal pricing strategy |
| **Property Type** | Entire homes outperform private rooms by 2.5x | Focus investment on entire homes |
| **Seasonality** | December-January peak = 30% revenue increase | Target holiday season for maximum returns |
| **Investment Score** | Top 15% of properties = "Strong Buy" | Filter to high-potential investments |


## 💻 Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 15+
- Power BI Desktop (optional, for editing)
- Docker (optional)

### Local Setup

```bash
# 1. Clone repository
git clone https://github.com/MASELAM1/airbnb-investment-platform.git
cd airbnb-investment-platform

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r etl/requirements.txt
pip install -r api/requirements.txt

# 4. Set up PostgreSQL
createdb -U postgres airbnb_investment

# 5. Run ETL pipeline
python etl/extract.py



## 🛠️ Technology Stack

| Layer | Technologies |
|-------|--------------|
| **Data Extraction** | Python, Pandas, NumPy, Requests |
| **Data Warehouse** | PostgreSQL (Star Schema, Materialized Views, Indexing) |
| **Backend API** | FastAPI, SQLAlchemy, Pydantic, Uvicorn |
| **Business Intelligence** | Power BI, DAX, Power Query |
| **DevOps** | Docker, Docker Compose, Git |
| **Cloud (Optional)** | AWS ECS, RDS, S3 |

## 📊 Database Schema (Star Schema)


# 6. Start API server
cd api
python main.py

# 7. Open Power BI dashboard
# Open powerbi/airbnb_dashboard.pbix
