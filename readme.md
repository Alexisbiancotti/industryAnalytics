# Data Architecture for the Modernization of Argentine Industrial PyMES
An adaptable and scalable data architecture designed to modernize small and medium-sized enterprises (PyMES) in Argentina's industrial sector. This project aims to optimize production processes, improve product quality, and promote digital transformation while addressing economic, technological, and human challenges.

# Table of Contents
- [Description](#Description)
- [Installation](#installation)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [ETL Process](#etl-process)
- [Machine Learning Integration](#machine-learning-integration)
- [Database and Backup](#database-and-backup)

---

## Description

This project focuses on designing, developing, and implementing a flexible data architecture tailored to the needs of an Argentine industrial company facing modernization challenges. The proposed architecture ensures scalability and adaptability to meet the company's specific operational demands and to operate within the financial and technical constraints typical of the Argentine industrial sector. By leveraging open-source software and incorporating technologies like IoT, BI tools, and predictive analytics, this solution provides a cost-effective pathway to enhance competitiveness and innovation in a resource-limited environment.


## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Alexisbiancotti/industryAnalytics.git
2.  Install Visual Studio Code for editing and development:
    
    *   [Download from here](https://code.visualstudio.com/).
        
    *   Open the cloned repository folder, right-click, and select "Open with Code."
        
3.  Create an .env file with the following variables
    ```bash
    AIRFLOW_UID=<Your User ID>
    GENERAL_USER=<Your Username>
    GENERAL_PASS=<Your Password>
4.  Install Docker Desktop:
    
    *   Follow the steps at Docker Desktop.
        
5.  Navigate to the project directory using PowerShell:
    ```bash
    cd path/to/industryAnalytics
6.  bashCopyEditdocker compose builddocker compose up -d
    ```bash
    docker compose build
    docker compose up -d   
7.  Access the Airflow webserver:
    
    *   Open your browser and go to http://localhost:8080.
        
    *   Use the credentials specified in your .env file.
        

## Usage
-----

### Data Interaction

*   Access the PostgreSQL database using tools like [DBeaver](https://dbeaver.io/).
    
*   Connect using the credentials in your .env file.
    

### Airflow DAGs

*   Visualize and execute DAGs directly from the Airflow interface.
    

### Power BI Dashboards

*   Analyze and visualize processed data using Power BI.
    

Data Sources
------------

### ERP System

*   Simulated using an API hosted in a Docker container (dummyAPI).
    
*   Endpoints provide JSON data for tables such as sales and items.
    

### IoT Sensors

*   Simulated using a Python script in the sensorGenerator container.
    
*   Generates temperature data for machines every minute.
    

ETL Process
-----------

### Data Extraction

*   **ERP System**:
    
    *   Managed by Airflow DAGs, which handle incremental, conditional, and full extractions.
        
*   **Sensors**:
    
    *   Data sent via API to the PostgreSQL server.
        

### Data Transformation

*   Performed using SQL views:
    
    *   stg\_consolidated: Consolidates metrics for Power BI.
        
    *   stg\_sensordata: Formats sensor data for analysis.
           

Machine Learning Integration
----------------------------

### Forecasting DAG

*   Uses Meta's Prophet model to predict sales trends based on time series data.
    
*   Stores forecasts in the forecast table for visualization in Power BI.
    







