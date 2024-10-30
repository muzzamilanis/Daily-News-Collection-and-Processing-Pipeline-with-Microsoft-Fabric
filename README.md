# Daily-News-Collection-and-Processing-Pipeline-with-Microsoft-Fabric

This project draws inspiration from YouTube video by Mr. K Talks Tech, titled [Build Your End-to-End Project in Just 3 Hours: Master Azure Data Engineering | Bing Data Analytics.](https://youtu.be/yHU9ADk10eQ?si=-_hUBjgTU65DZ35a). Full credit goes to them for the original content and inspiration.

This project provides an end-to-end solution for gathering and analyzing daily news content using Microsoft Fabric’s Saas based architecture data engineering platform. This project utilizes the [Bing Web Search API](https://www.microsoft.com/en-us/bing/apis/bing-web-search-api) to collect recent news articles, perform sentiment analysis, and present the insights via an interactive Power BI dashboard.

## Technologies Used

Project is based on Microsoft Fabric. The primary components of this pipeline include Microsoft Fabric's: 
- **Data Ingestion**
  - Data Factory
- **Storage**
  - Lakehouse (both raw files and tables)
- **Processing and orchestration**
  - Jupyter Notebooks (PySpark)
- **Visualization**
  - PowerBI
- **Alerts and Notifications**
  - Data Activator
each component playing a vital role in the seamless flow of data collection, processing, and visualization. Automated alerts ensure timely access to new data, allowing for up-to-date reviews of news sentiment and trends.
