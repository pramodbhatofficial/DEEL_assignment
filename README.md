# Deel Data Engineering Take-Home Assignment


# Project Overview
This project implements a simple data system as per the Deel Data Engineering take-home exercise. The goal is to ingest raw organization and invoice data from Snowflake, transform it using dbt to create reliable analytical models, and implement a function to alert when an organization's daily financial balance changes by more than 50% day-over-day.

The pipeline focuses on simplicity, reliability, and efficiency, leveraging:

Snowflake: As the data warehouse for source data and dbt model storage.
dbt (Cloud/Core): For data transformation, testing, and documentation.
Python: For data loading (optional script) and the final alerting logic.
Git/GitHub: For version control.

# Repository Structure
```markdown
DEEL_assignment/
|-- my_new_project/      <-- Your dbt project folder (rename if needed)
|   |-- models/
|   |   |-- staging/
|   |   |-- intermediate/
|   |   |-- marts/
|   |-- tests/
|   |-- dbt_project.yml
|   |-- packages.yml
|   |-- ... (other dbt files)
|-- scripts/             <-- Folder for Python utility scripts
|   |-- upload_to_stage.py  <-- Script to upload local CSV to Snowflake stage
|   |-- alerter.py          <-- Script to check balance changes and alert
|-- .gitignore
|-- README.md
````

