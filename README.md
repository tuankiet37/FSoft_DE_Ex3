
# FSoft_DE_Ex3

This project demonstrates a Data Engineering pipeline for fetching and storing weather data for cities in Vietnam. The pipeline utilizes the public API from [`api.open-meteo.com`](https://api.open-meteo.com/) to extract real-time weather data and stores the processed results in a PostgreSQL database.

The API is called using the following parameterized URL:

```python
f'/v1/forecast?latitude={city["lat"]}&longitude={city["long"]}&current_weather=true'
```

## Implementation

The solution involves two approaches:
1. **Custom Airflow Installation**: A custom-built Airflow environment located in the `airflow` folder.
2. **Astro**: An enhanced version of Airflow, showcasing its newer features.

---

## Pipeline

### 1. **Extract**:
- Data is requested from the Open Meteo API (`api.open-meteo.com`) for each city's latitude and longitude in Vietnam.
- Real-time weather information is retrieved, including parameters such as temperature, wind speed, and weather conditions.

### 2. **Transform**:
- The raw weather data is cleaned and formatted into a consistent structure.
- Validations are applied to ensure the data is complete and ready for database insertion.

### 3. **Load**:
- A PostgreSQL connection is established.
- If not already exits, a table is created in the PostgreSQL database to store the weather data.
- Transformed data is inserted into the database for later analysis.

---

## DAGs
Below is the flow of the task in DAGs:

```mermaid
graph LR;
    A[Read lat/long of cities in Vietnam] ;
    A --> B[Extract API Response for Weather Data];
    B --> C[Transform Weather Data];
    C --> D[Load Transformed Data into PostgreSQL];
```
---


## Technologies Used

- **Airflow**: Orchestrates the data pipeline and ensures task dependencies are met.
- **Astro**: A modern platform built on top of Airflow for improved workflows.
- **PostgreSQL**: Serves as the storage layer for processed weather data.
- **API Integration**: Fetches weather data from a public API.

---


