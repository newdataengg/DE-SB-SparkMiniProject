# 🚗 Spark Mini Project: Automobile Post-Sales Accident Report

This project processes a dataset of vehicle incident history to generate a report showing the number of accidents grouped by car make and year. It uses Apache Spark to demonstrate how in-memory processing outperforms traditional Hadoop MapReduce.

---

## 📁 Dataset

Input file: `data.csv`  
Expected columns:
incident_id, incident_type, vin_number, make, model, year, incident_date, description



Incident types:
- `I`: Initial sale (contains make, model, year)
- `A`: Accident (missing make/year)
- `R`: Repair

---

## 🧠 Goal

Generate a summary report of accident counts for each (make, year) combination.

Example output:
Nissan-2003,1
BMW-2008,10
MERCEDES-2013,2


---

## 📦 Requirements

- Python 3.x
- Apache Spark 3.x
- `data.csv` in the same directory or HDFS path

---

## 🚀 Running the Project

1. **Add your `data.csv` file** to your working directory.

2. **Run the Spark job** using:

```bash
spark-submit autoinc_spark.py

cat output/part-*



