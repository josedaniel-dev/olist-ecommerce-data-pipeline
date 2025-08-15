# 📦 Sprint 1 – AnyoneAI 2025 [Olist Ecommerce Data Pipeline]

## 📌 Project Overview

This project is part of **AnyoneAI's Sprint Project 01** and focuses on building a **data pipeline** for cleaning, extracting, loading, and transforming e-commerce order data, followed by generating visual insights in a Jupyter Notebook.

The final deliverables include:

* **Automated pipeline** to clean and load datasets into SQLite
* **Predefined SQL transformations** to generate insights
* **Automated test validations** to verify query outputs
* **Data visualization notebook** with charts, tables, and maps

---

## 🗂 Repository Structure

```
.
├── artifacts/                  # SQLite database output
├── cleaned_data/               # Clean CSV files after preprocessing
├── notebooks/
│   └── AnyoneAI - Sprint Project 01.ipynb
├── scripts/
│   ├── run_pipeline.py         # Main ETL pipeline runner
│   ├── dump_query_results.py   # Save SQL query outputs to JSON
├── src/
│   ├── extract.py              # Load CSV data into DataFrames
│   ├── load.py                 # Load DataFrames into SQLite
│   ├── transform.py            # SQL query execution functions
│   └── config.py               # Paths & settings
├── tests/
│   └── query_results/          # Expected outputs for pytest
├── requirements.txt
├── README.md
└── ASSIGNMENT.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the repository

```bash
git clone https://github.com/josedaniel-dev/sprint1_anyoneai_2025.git
cd sprint1_anyoneai_2025
```

### 2️⃣ Create and activate a virtual environment

```bash
python3 -m venv sprint1
source sprint1/bin/activate     # Mac/Linux
sprint1\Scripts\activate        # Windows
```

### 3️⃣ Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 🚀 Running the Pipeline

The pipeline will:

1. Clean the raw CSVs
2. Extract data into Pandas
3. Load into SQLite database
4. Run predefined queries
5. Save outputs to `tests/query_results/`

```bash
PYTHONPATH=. python scripts/run_pipeline.py --db-path artifacts/olist.sqlite
```

---

## 🧪 Running Tests

Tests validate SQL queries against expected results.

```bash
pytest -v
```

---

## 📊 Running the Notebook

Launch Jupyter Notebook:

```bash
jupyter notebook
```

Then open:

```
notebooks/AnyoneAI - Sprint Project 01.ipynb
```

This notebook contains:

* **Data overview**
* **Exploratory analysis**
* **Bar charts, pie charts, maps**
* **Insights discussion**

---

## 📈 Example Visualizations

* Revenue per state (choropleth map)
* Orders per status (bar chart)
* Delivery time differences (histogram)
* Freight value vs. product weight (scatter plot)

---

## 📝 Notes

* This repo avoids committing large CSVs; regenerate them via the pipeline.
* SQLite database (`olist.sqlite`) is generated in `/artifacts`.



