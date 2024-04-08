# App analysis tool for competitive intelligence

This web application is designed to scrape application data from Google Play and the App Store based on user-defined criteria. The scraped data is transformed for analysis, saved to CSV, and uploaded to Google Cloud Storage. A custom dashboard created in Google Looker then visualizes this data.

This project is a part of thesis included in thesis "Analýza trhu s mobilními aplikacemi s využitím Competitive Intelligence a jeho nástrojů".
Prague University of Economics and Business

## Getting Started

These instructions will give you a copy of the project up and running on
your local machine for development and testing purposes. See deployment
for notes on deploying the project on a live system.


## Features

- **Data Scraping**: Dynamically scrapes app data from Google Play and the App Store using the `google-play-scraper` and `app-store-scraper` libraries.
- **User Input**: Allows users to define scraping criteria through a web interface.
- **Data Transformation**: Cleans, categorizes, and enhances the data using Python with Pandas, TextBlob, and iso639 libraries.
- **Data Storage**: Saves the transformed data to CSV files and uploads them to Google Cloud Storage.
- **Visualization**: A custom Google Looker dashboard fetches the data for visualization and analysis.



## How It Works

1. **User Interaction**: The user inputs their search criteria via the frontend, which is developed with HTML, CSS, and Vanilla JavaScript.
2. **Scraping**: The backend, powered by NodeJS, scrapes the app data based on the user's input.
3. **Data Transformation**: The scraped data is saved as a CSV file and then transformed using Python. This includes cleaning, categorization, and calculation of new fields relevant for analysis.
4. **Storage and Visualization**: The final CSV file is uploaded to Google Cloud Storage. A custom dashboard in Google Looker then accesses this data via a connector for visualization.


### Prerequisites

- Node.js
- Python 3.x
- Access to Google Cloud Storage and Google Looker with service account


## Author

  - **Bc.Hoang Nam Dao**
    [robogentlenam](https://github.com/robogentlenam)



## Acknowledgments
  - PhDr. Jan Černý, Ph.D. for leading the thesis
  
