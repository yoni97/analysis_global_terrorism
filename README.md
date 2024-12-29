# Social Network Analysis Backend Project

This project involves building a robust backend system for analyzing suspicious social network data. The system leverages multiple microservices, advanced data analytics, and visualizations to provide insightful reports. Below is a detailed overview of the project and its implementation.

---

## **Table of Contents**

1. [Overview](#overview)
2. [Tech Stack](#tech-stack)
3. [Project Structure](#project-structure)
4. [Endpoints](#endpoints)
5. [Data Processing](#data-processing)
6. [Setup and Installation](#setup-and-installation)
7. [Usage Instructions](#usage-instructions)
8. [Features](#features)
9. [Future Improvements](#future-improvements)

---

## **Overview**

The backend system aims to:

- Analyze social network activity to identify key influencers, regions with high intergroup activity, and shared strategies among groups.
- Store and retrieve data efficiently using databases like PostgreSQL, Neo4j, Redis, and MongoDB.
- Process data in real-time using Kafka for messaging.
- Visualize results using maps and graphs.

This project integrates Flask for the API layer, Pandas for data processing, and Folium for map visualizations.

---

## **Tech Stack**

- **Programming Language:** Python 3.12.2
- **Web Framework:** Flask
- **Databases:**
  - PostgreSQL
  - Neo4j (Graph Database)
  - Redis (In-memory store)
  - MongoDB
- **Data Analytics and Visualization:**
  - Pandas
  - Matplotlib
  - Folium
- **Messaging System:** Apache Kafka
- **Containerization:** Docker, Docker Compose

---
 README.md                # Project documentation
```

---

## **Endpoints**

### **Main API Endpoint**

`/api`
- Handles analysis requests based on query parameters such as `function`, `filter_type`, and `display_option`.

#### Example Request:
```bash
GET /api?function=find_influential_groups&region=Middle East & North Africa
```

#### Supported Functions:
- **find_influential_groups**: Identifies key influencers.
- **get_average_wounded_by_region**: Calculates average wounded by region.
- **get_terror_incidents_change**: Analyzes changes in incidents over time.
- **get_top_active_groups**: Highlights the most active groups.
- **find_shared_attack_strategies**: Reveals shared strategies among groups.

### example map output:
- https://1drv.ms/v/c/cf69c709bc7e15b8/EVhx3JU_lgNGvDepV7QJEgQB6tdLSqLpE0yiV24aO9GibA
- https://1drv.ms/v/c/cf69c709bc7e15b8/ESkmqSHqScBEv7k87jm3i8QBpIGa8y79KG-5tgXow4PAEw

### **Visualization Endpoints**

`/api/<visualization>`
- Renders maps and graphs for visual analysis.
- Example: `/api/top_active_groups_map`

---

## **Data Processing**

1. **Loading CSV Data**
   - CSV files are loaded into Pandas DataFrames for processing during app initialization.
   - Example data processing includes:
     - Removing NaN values in key columns.
     - Filtering data based on user-specified regions or countries.

2. **Data Aggregation**
   - Grouping data by specific criteria (e.g., regions, groups).
   - Calculating influence scores, trends, and unique attributes.

3. **Visualization**
   - Generated using Folium (for maps) and Matplotlib (for statistical graphs).
   - Saved as HTML files for easy sharing and embedding.

---

## **Setup and Installation**

### **Pre-requisites**
- Docker
- Python 3.12.2
- PostgreSQL, MongoDB, Neo4j installed or running via Docker

### **Steps**

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd project
   ```

2. Build and run containers:
   ```bash
   docker-compose up --build
   ```

3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Run the Flask app:
   ```bash
   python app.py
   ```

---

## **Usage Instructions**

1. Access the API endpoints via Postman or browser.
2. Example query to find influential groups:
   ```bash
   GET http://127.0.0.1:5000/api?function=find_influential_groups&region=Middle East & North Africa
   ```
3. View the generated maps in the `templates` directory.

---

## **Features**

- **Data Cleaning:** Ensures only valid rows are processed by filtering NaN values and invalid coordinates.
- **Customizable Filters:** Allows filtering by regions and countries.
- **Influence Analysis:** Calculates scores based on unique regions and targets.
- **Real-time Messaging:** Kafka integration for real-time data streaming.
- **Database Integration:** Stores results for persistence and advanced querying.
- **Interactive Visualizations:** Generates maps and statistical graphs dynamically.

---

## **Future Improvements**

- **Enhanced Filtering:** Support for multi-region and multi-country filters.
- **User Authentication:** Secure access to API endpoints.
- **Advanced Visualizations:** Add animations and interactive dashboards.
- **Performance Optimization:** Use distributed computing for large datasets.
- **Machine Learning:** Integrate predictive analytics for early detection of trends.

---

## **Contributing**

We welcome contributions! Please fork the repository and submit a pull request with your changes. Ensure your code adheres to the project’s coding standards.

---
