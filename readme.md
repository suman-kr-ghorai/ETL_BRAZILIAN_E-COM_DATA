#  ETL + Analytics for Brazilian E-Commerce Data

This project is an end-to-end **ETL pipeline and dashboard** for analyzing **Brazilian e-commerce data** using **Streamlit, BigQuery, and Kaggle Olist datasets**.

---

##  Project Overview

- Extracts data from *Kaggle/MySQL/CSV*
- Transforms & cleans e-commerce records
- Builds a **star schema** (fact & dimension tables)
- Uploads data to **BigQuery**
- Generates **aggregations** and **data marts**
- Visualizes KPIs using **Streamlit dashboards**

---

##  Project Structure

```bash
.
├── ETL.py                    # Streamlit app (manual + automated ETL)
├── utils/
│   ├── aggregate_utils.py     # Creates aggregation tables
│   ├── bigquery_upload_utils.py # Uploads fact/dim tables to BigQuery
│   ├── load_datamart_utils.py # Builds data mart tables
│   ├── schema_utils.py        # Builds star schema from merged data
│   └── ... (merge, clean, fetch helpers)
├── data/                      # Contains CSVs extracted from Kaggle
├── app.log                    # Execution log file
├── requirements.txt           # Python dependencies
├── .env.example               # GCP credentials/config template
├── visualizations/            # Stores generated graphs
├── reports/                   # Generated PDF reports for mailing
├── pages/
│   ├── Aggregate-Insights.py   # Insights from aggregate tables
│   ├── DataMart-Insights.py    # Data mart insights
│   ├── Kpi-And-Conclusion.py   # KPI insights and conclusions
```

---

##  Setup & Installation

### 1️ Clone the Repository

```bash
git clone https://github.com/your-username/ecommerce-etl-analytics.git
cd ecommerce-etl-analytics
```

### 2️ Install Dependencies

Create a virtual environment (optional but recommended):

```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

Then install the required dependencies:

```bash
pip install -r requirements.txt
```

### 3️ Set Up Kaggle API

1. Sign in to [Kaggle](https://www.kaggle.com/).
2. Navigate to **Account Settings** and select **Create API Token**.
3. Download the `kaggle.json` file.
4. Place the file in the `.kaggle/` directory inside your home folder:

   ```bash
   mkdir -p ~/.kaggle
   mv kaggle.json ~/.kaggle/
   chmod 600 ~/.kaggle/kaggle.json
   ```

5. Run the following command to download the dataset:

   ```bash
   kaggle datasets download -d olistbr/brazilian-ecommerce
   unzip brazilian-ecommerce.zip -d data/
   ```

### 4️ Set Up Google Cloud BigQuery

1. [Create a Google Cloud project](https://console.cloud.google.com/).
2. Enable **BigQuery API**.
3. Install and authenticate with **Google Cloud SDK**:

   ```bash
   pip install --upgrade google-cloud-bigquery
   gcloud auth application-default login
   ```

4. Copy `.env.example` to `.env` and fill in your BigQuery credentials.

### 5️⃣ Run the ETL Pipeline

Run the ETL script to extract, transform, and load data into BigQuery:

```bash
python ETL.py
```

### 6️⃣ Start the Streamlit Dashboard

```bash
streamlit run ETL.py
```

This will launch the dashboard in your browser.

---

##  Features

- **Automated ETL:** Fetches data from Kaggle, processes it, and loads it into BigQuery.
- **Star Schema:** Optimized for analytical queries.
- **Streamlit Dashboards:** Provides insights from aggregated data.
- **BigQuery Integration:** Supports scalable cloud-based analytics.

---

##  Contact
For any questions or issues, please open an issue on GitHub or reach out to me at **suman.kr.ghorai@gmail.com**.

---



