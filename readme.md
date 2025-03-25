# 🛍 ETL + Analytics for Brazilian E-Commerce Data

This project is an end-to-end *ETL pipeline and dashboard* for analyzing *Brazilian e-commerce data* using *Streamlit, **BigQuery, and **Kaggle Olist datasets*.

---

## 📊 Project Overview

- Extracts data from *Kaggle/MySQL/CSV*
- Transforms & cleans e-commerce records
- Builds a *star schema* (fact & dimension tables)
- Uploads data to *BigQuery*
- Generates *aggregations* and *data marts*
- Visualizes KPIs using *Streamlit dashboards*

---

## 🧱 Project Structure

```bash
.
├── ETL.py                    # Streamlit app (manual + automated ETL)
├── utils/
│   ├── aggregate_utils.py     # Creates aggregation tables
│   ├── bigquery_upload_utils.py # Uploads fact/dim tables to BigQuery
│   ├── load_datamart_utils.py # Builds data mart tables
│   ├── schema_utils.py        # Builds star schema from merged data
│   └── ... (merge, clean, fetch helpers)
├── data/                      # Contains CSVs extracted from kaggle
├── app.log                    # Execution log file
├── requirements.txt           # Python dependencies
├── .env.example               # GCP credentials/config template
├── visualizations             #stores graphs
├── reports                    # generated pdf report for mailing
├── requirements.txt
├── pages/
       ├──  Aggregate-Insights.py   #insights from aggreagate tables
       ├── DataMart-Insights.py
       ├── Kpi-And-Conclusion.py

