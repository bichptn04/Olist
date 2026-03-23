## Summary

This project develops an end-to-end data analytics and data warehouse solution using the Olist e-commerce dataset. 

SQL was used to design and implement a star schema, including fact and dimension tables, while supporting ETL processes for data extraction, transformation, and loading. RFM (Recency, Frequency, Monetary) metrics were calculated to evaluate customer value and behavior. 

Followed by analytical querying and interactive dashboard development in Power BI. The project focuses on analyzing sales performance and customer behavior through key business metrics such as revenue, order trends, average order value (AOV), and customer retention. 

Additionally, Python was applied to perform in-depth RFM analysis including aggregation, and scoring of customers based on recency, purchase frequency, and monetary value. Customers were then segmented into meaningful groups (e.g., high-value, loyal, at-risk) using RFM scoring logic, enabling better understanding of customer distribution and behavior patterns.

By combining SQL-based data modeling, Python-driven customer segmentation, and Power BI visualization, the project delivers actionable insights to support data-driven decision-making, including improving customer retention strategies, optimizing sales performance, and identifying high-value customer segments.
## This project aims to
- Analyze sales performance
- Understand customer behavior
- Provide data-driven insights to improve business 
## About the Dataset
It is a public dataset and it has information of about 100 thousand orders from 2016 to 2018 You can find it in Kaggle [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

![Schema](./Visualizations/Dataset%20Schema.png)

## Data Pipeline
Raw CSV Files → Staging Tables → Data Warehouse (Star Schema) → Power BI Dashboard
- Load raw data into staging tables
- Clean and transform data using SQL
- Build fact and dimension tables
- Connect to Power BI for visualization
## Star Schema (SQL)

![Star Schema](./Visualizations/Star%20Schema.png)
This structure improves query performance and simplifies analytical reporting.
## Dashboard (Power BI)
The Power BI dashboard provides an interactive overview of sales performance and customer behavior.
**Sales Overview**: Displays key metrics such as total revenue, orders, customers, and sales trends over time to identify growth patterns and seasonality.
**Customer Analysis**: Highlights customer metrics including retention, average order value (AOV), and segmentation based on RFM to identify high-value and at-risk customers.

![Overview](./Visualizations/Sales%20Performance%20Dashboard-Overview.jpg)

![Customer](./Visualizations/Sales%20Performance%20Dashboard-Customer.jpg

## Customer Segmentation (Python)
Python was used to perform advanced customer segmentation by combining RFM analysis with clustering techniques. After preprocessing and aggregating transactional data at the customer level, Recency, Frequency, and Monetary (RFM) metrics were calculated and normalized.
K-Means clustering was then applied to group customers into distinct segments based on their purchasing behavior. The optimal number of clusters was determined using Elbow Method. Each cluster represents a specific customer profile, such as low value, high-value, loyal, and churned customers.

This approach provides a more data-driven and scalable segmentation compared to rule-based methods, enabling deeper insights into customer distribution and supporting targeted marketing strategies to improve retention and maximize revenue.
## Key Insights
There is no significant recurring drop or spike across the same months in different years, suggesting that seasonality impact is limited.
Customer retention remains relatively low (3.1%), as many customers make only one or few purchases.
A significant portion of customers falls into low-frequency and low-monetary segments, suggesting opportunities for engagement and upselling.
Segmentation results enable targeted strategies, such as rewarding high-value customers and re-engaging inactive users to improve overall revenue performance.






