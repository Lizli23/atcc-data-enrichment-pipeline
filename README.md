
# ATCC Data Enrichment Pipeline

This project builds an end-to-end data pipeline to collect, clean, and enrich biological cell line data from the ATCC database. The goal is to transform raw, unstructured data into a structured dataset suitable for downstream analysis and machine learning applications. (I selected only the key files to upload here for demonstration purpose) 


---

## Overview

Biological datasets are often messy, inconsistent, and difficult to use directly. This project addresses that by:

- Collecting data from the ATCC website through automated web scraping
- Merging and standardizing multiple datasets
- Enriching data with additional features such as product type and application
- Cleaning and formatting outputs for reliable downstream use
- Performing basic exploratory analysis

---

## Project Structure

- `scrape_atcc.py`  
  Scrapes cell line data from the ATCC website using Selenium and BeautifulSoup. Handles pagination and collects detailed product-level information.

- `enrich_atcc_fixed.py`  
  Processes and enriches the dataset by:
  - Standardizing ATCC IDs from source URLs
  - Extracting product type and application fields
  - Classifying whether each record is a true cell line
  - Handling encoding issues and messy real-world data
  - Writing Excel-safe CSV outputs

- `analysis.ipynb`  
  Performs basic exploratory data analysis, including summary statistics and visualization of the processed dataset.

---

## Key Features

- Automated web scraping with pagination handling
- Robust retry logic and request handling
- Data cleaning and normalization for real-world datasets
- Feature engineering for downstream machine learning use
- Resume-safe pipeline design for large datasets
- Excel-safe CSV output formatting

