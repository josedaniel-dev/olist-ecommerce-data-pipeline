# 📊 ETL Pipeline – AnyoneAI Sprint Project 01

This repository contains the implementation of an **ETL pipeline (Extract → Transform → Load)** built in **Python 3.8.10**.  
The pipeline extracts raw CSV data, transforms it using SQL queries, and loads the results into a database for further analysis.  

---

## 📂 Project Structure
```

assignment/
├── requirements.txt        # Project dependencies
├── src/                    # Pipeline source code
│   ├── **init**.py
│   ├── config.py           # Configuration and paths
│   ├── extract.py          # Data extraction and initial cleaning
│   ├── transform.py        # SQL queries and transformations
│   ├── load.py             # Load into database
│   ├── utils.py            # Helper functions
│   └── run\_pipeline.py     # Main entrypoint to run the ETL
├── queries/                # SQL files used in transformations
├── dataset/                # Original CSV datasets
└── tests/                  # Unit tests with pytest

````

---

## ⚙️ Requirements
- Python **3.8.10**  
- Pip (latest version)  
- All dependencies are pinned in `requirements.txt`  

---

## 🚀 Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/josedaniel-dev/etl-pipeline.git
   cd etl-pipeline
````

2. Create and activate a virtual environment:

   ```bash
   python -m venv venv_38
   source venv_38/bin/activate   # Linux / macOS
   venv_38\Scripts\activate      # Windows
   ```

3. Install dependencies:

   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

---

## ▶️ Running the Pipeline

Execute the full ETL workflow:

```bash
python -m src.run_pipeline
```

Steps performed:

1. **Extract** → Reads CSV files from `dataset/` and applies initial cleaning.
2. **Transform** → Runs SQL queries stored in `queries/`.
3. **Load** → Saves transformed data into the database.

---

## 🧪 Tests

Unit tests are included with **pytest**:

```bash
pytest -q
```

All tests should pass ✅.

---

## 📌 Notes

* Tested with **Python 3.8.10**, `pandas==1.5.3`, and `SQLAlchemy==1.4.52`.
* Do not commit virtual environments (`venv/`), `__pycache__/`, or temporary files.
* The `queries/` folder contains all SQL transformations.
* The `dataset/` folder contains the original CSV files.

---

## 👨‍💻 Author

**Jose Daniel Soto**

* GitHub: [josedaniel-dev](https://github.com/josedaniel-dev)
* LinkedIn: [linkedin.com/in/josedanielsoto](https://www.linkedin.com/in/josedanielsoto/)

---

✨ Developed as part of the **AnyoneAI Program**.

---