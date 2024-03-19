import pandas as pd
from textblob import TextBlob
from ast import literal_eval
import re
from datetime import datetime, timezone
import json
import argparse


# Example transformation function
def transform_AppStoreData(input_file, output_file):

# Loading and Preprocessing the Dataset
    df = pd.read_csv('./AppStoreOutput.csv', delimiter=';', encoding='utf-8')
    df['released'] = pd.to_datetime(df['released'])
    df['updated'] = pd.to_datetime(df['updated'])
    df['score'] = pd.to_numeric(df['score'], errors='coerce')
    df['free'] = df['free'].astype(int)
    df['supports_iPhone'] = 0
    df['supports_iPad'] = 0
    df['supports_Mac'] = 0


    # Data Cleaning and Transformation
    def clean_review_text(text):
        if not isinstance(text, str):
            return ""
        cleaned_text = re.sub(r'[^\u0000-\u007F]+', '', text)
        return cleaned_text

    df['reviews'] = df['reviews'].apply(clean_review_text)

    for index, row in df.iterrows():
        if 'iPhone' in row['supportedDevices']:
            df.at[index, 'supports_iPhone'] = 1
        if 'iPad' in row['supportedDevices']:
            df.at[index, 'supports_iPad'] = 1
        if 'Mac' in row['supportedDevices']:
            df.at[index, 'supports_Mac'] = 1

    # Sentiment Analysis
    def compute_sentiment_category(text):
        try:
            sentiment = TextBlob(str(text)).sentiment.polarity
            if sentiment < -0.2:
                return 'Negative'
            elif sentiment < 0:
                return 'Slightly negative'
            elif sentiment == 0:
                return 'Neutral'
            elif sentiment <= 0.2:
                return 'Slightly positive'
            else:
                return 'Positive'
        except:
            return 'Missing'

    df['Sentiment_Category'] = df['reviews'].apply(compute_sentiment_category)

    # Parsing and One-hot Encoding for List Columns
    def parse_list_column(column):
        try:
            return column.apply(literal_eval)
        except ValueError:
            return column

    df['languages'] = parse_list_column(df['languages'])
    df['genres'] = parse_list_column(df['genres'])
    languages_exploded = df.explode('languages')
    genres_exploded = df.explode('genres')
    languages_one_hot = pd.get_dummies(languages_exploded['languages'], prefix='lang')
    genres_one_hot = pd.get_dummies(genres_exploded['genres'], prefix='')
    languages_encoded = languages_one_hot.groupby(languages_one_hot.index).sum()
    genres_encoded = genres_one_hot.groupby(genres_one_hot.index).sum()
    df = df.join([languages_encoded, genres_encoded])

    # Categorizing Numerical Data
    bins = [0, 50000000, 200000000, float('inf')]
    labels = ['Small', 'Medium', 'Large']



    ## App age categorization
    def categorize_app_age(days):
        if days <= 30:
            return 'Brand New'
        elif days <= 90:
            return 'Recently Launched'
        elif days <= 365:
            return 'Established'
        elif days <= 1095:
            return 'Mature'
        else:
            return 'Very Mature'
        
    ## Price categorization
    def categorize_price(price):
        if price == 0:
            return 'Free'
        elif price < 1:
            return 'Low price'
        elif price <= 10:
            return 'Medium price'
        else:
            return 'High price'
            

    ## Update frequency categorization
    def categorize_update_frequency(days_since_last_update):
        categories = {
            (days_since_last_update <= 30): 'Very Recent Updates',
            (days_since_last_update <= 90): 'Recently Updated',
            (days_since_last_update <= 180): 'Moderately Updated',
            (days_since_last_update <= 365): 'Rarely Updated',
            (days_since_last_update > 365): 'Stale'
        }
        return next(value for key, value in categories.items() if key)


    #Calculations 
    df['days_since_last_update'] = (datetime.now(timezone.utc) - df['updated']).dt.days
    df['update_frequency'] = df['days_since_last_update'].apply(categorize_update_frequency)
    df['app_age'] = (df['updated'] - df['released']).dt.days





    ## Apply the categorization function to the 'price' column
    df['price_category'] = df['price'].apply(lambda price: categorize_price(price))

    ## Apply the categorization function to the 'app_age' column
    df['app_age_category'] = df['app_age'].apply(lambda days: categorize_app_age(days))

    ## Apply the categorization function to the 'file_size' column
    df['file_size_category'] = pd.cut(df['size'], bins=bins, labels=labels)



    # Final DataFrame Cleanup and Saving the Cleaned Data
    columns_to_remove = [
        'id', 'appId', 'url', 'description', 'icon', 'genreIds', 'primaryGenreId',
        'requiredOsVersion', 'releaseNotes', 'version', 'developerid', 'developerUrl',
        'developerWebsite', 'screenshots', 'ipadScreenshots', 'appletvScreenshots',
        'languages', 'genres', 'supportedDevices', 'currency', 'developerId', 'reviews', 'score', 
    ]
    df.drop(columns_to_remove, axis=1, inplace=True, errors='ignore')

    # Save the modified DataFrame to a new CSV file
    df.to_csv('AppStoreOutput_cleaned.csv', index=False, sep=';', encoding='utf-8')

    # Preview the DataFrame
    print(df.head())

    pass

def transform_GooglePlayData(input_file, output_file):

    # Load and transform data
    df = pd.read_csv('./GooglePlayOutput.csv', delimiter=';', encoding='utf-8')
    df['released'] = pd.to_datetime(df['released']).dt.tz_localize('UTC')
    df['updated'] = pd.to_datetime(df['updated'], unit='ms', utc=True)

    # Clean data
    df['contentRating'] = df['contentRating'].str.replace('Rated for', '', regex=False).str.strip()
    df['score'] = pd.to_numeric(df['score'], errors='coerce')
    df['free'] = df['free'].astype(int)

    # Feature Engineering
    ## Calculate Days Since Last Update

    ## Install to rating ratio categorization
    def categorize_install_to_rating_ratio(ratio):
        if ratio <= 100:  # Assuming 1 rating per 100 installs or less is high feedback
            return 'High Review Ratio'
        elif ratio <= 500:  # Assuming between 100 and 500 installs per rating is moderate feedback
            return 'Moderate Review Ratio'
        else:  # More than 500 installs per rating is considered low feedback
            return 'Low Review Ratio'
        
    #Sentiment Analysis and Categorization
    def compute_sentiment_category(text):
        try:
            sentiment = TextBlob(str(text)).sentiment.polarity
            if sentiment < -0.2:
                return 'Negative'
            elif sentiment < 0:
                return 'Slightly negative'
            elif sentiment == 0:
                return 'Neutral'
            elif sentiment <= 0.2:
                return 'Slightly positive'
            else:
                return 'Positive'
        except:
            return 'Missing'  # Assuming neutral for non-text entries or errors    

    ## Rating ratio categorization
    def categorize_rating_ratio(ratio):
        if ratio > 10:
            return 'Exceptional'
        elif ratio > 5:
            return 'Great'
        elif ratio > 2:
            return 'Good'
        elif ratio > 1:
            return 'Mixed'
        else:
            return 'Poor'

    ## App age categorization
    def categorize_app_age(days):
        if days <= 30:
            return 'Brand New'
        elif days <= 90:
            return 'Recently Launched'
        elif days <= 365:
            return 'Established'
        elif days <= 1095:
            return 'Mature'
        else:
            return 'Very Mature'

    ## Price categorization
    def categorize_price(price):
        if price == 0:
            return 'Free'
        elif price < 1:
            return 'Low price'
        elif price <= 10:
            return 'Medium price'
        else:
            return 'High price'
        

    ## Engagement score categorization
    def categorize_engagement_score(score, percentiles):
        if score >= percentiles[0.9]:
            return 'Very High Engagement'
        elif score >= percentiles[0.75]:
            return 'High Engagement'
        elif score >= percentiles[0.5]:
            return 'Moderate Engagement'
        elif score >= percentiles[0.25]:
            return 'Low Engagement'
        else:
            return 'Very Low Engagement'

    ## Update frequency categorization
    def categorize_update_frequency(days_since_last_update):
        categories = {
            (days_since_last_update <= 30): 'Very Recent Updates',
            (days_since_last_update <= 90): 'Recently Updated',
            (days_since_last_update <= 180): 'Moderately Updated',
            (days_since_last_update <= 365): 'Rarely Updated',
            (days_since_last_update > 365): 'Stale'
        }
        return next(value for key, value in categories.items() if key)


    ## Category parsing
    def parse_categories(row):
        try:
            categories = json.loads(row)
        except json.JSONDecodeError:
            return pd.Series()
        return pd.Series({category['name']: 1 for category in categories})
    categories_expanded = df['categories'].apply(parse_categories).fillna(0).astype(int)
    df = df.join(categories_expanded)

    ## Sentiment analysis
    df['sentiment_category'] = df['reviews'].apply(lambda text: compute_sentiment_category(text))

    ## Histogram parsing
    def parse_histogram(row):
        try:
            histogram_dict = json.loads(row)
        except json.JSONDecodeError:
            return pd.Series([float('nan')] * 5)
        return pd.Series(histogram_dict)
    histogram_columns = df['histogram'].apply(parse_histogram)
    histogram_columns.columns = ['1*', '2*', '3*', '4*', '5*']
    df = pd.concat([df, histogram_columns], axis=1)

    ## Additional Calculations
    df['install_to_rating'] = df['minInstalls'] / (df['ratings'] + 1e-10)
    df['engagement_score'] = (df['score'] * df['ratings']) / df['minInstalls']
    df['rating_ratio'] = (df['4*'] + df['5*']) / (df['1*'] + df['2*'])
    df['days_since_last_update'] = (datetime.now(timezone.utc) - df['updated']).dt.days
    df['app_age'] = (df['updated'] - df['released']).dt.days
    df['update_frequency'] = df['days_since_last_update'].apply(categorize_update_frequency)


    ## Categorizations
    df['app_age_category'] = df['app_age'].apply(lambda days: categorize_app_age(days))
    df['rating_ratio_category'] = df['rating_ratio'].apply(lambda ratio: categorize_rating_ratio(ratio))
    percentiles = df['engagement_score'].quantile([0.25, 0.5, 0.75, 0.9]).to_dict()
    df['engagement_score_category'] = df['engagement_score'].apply(lambda x: categorize_engagement_score(x, percentiles))
    df['price_category'] = df['price'].apply(lambda price: categorize_price(price))
    df['install_to_rating_category'] = df['install_to_rating'].apply(categorize_install_to_rating_ratio)

    # Clean-up and Output
    columns_to_remove = [
        'description', 'descriptionHTML', 'summary', 'installs', 'maxInstalls', 'scoreText', 'reviews', 'histogram', 'currency', 'androidVersion', 'androidVersionText',
        'androidMaxVersion', 'previewVideo', 'developerId', 'developerEmail', 'developerWebsite', 'developerAddress', 'privacyPolicy', 'developerInternalID', 'genreId', 'icon', 'headerImage',
        'screenshots', 'video', 'videoImage','contentRatingDescription','version', 'recentChanges', 'comments', 'appId', 'url', 'originalPrice', 'discountEndDate', 'categories',  'priceText',
        '1*', '2*', '3*', '4*', '5*', 
    ]
    df.drop(columns_to_remove, axis=1, inplace=True, errors='ignore')
    df['updated'] = df['updated'].dt.strftime('%Y-%m-%d')
    df['released'] = df['released'].dt.strftime('%Y-%m-%d')
    df.to_csv('GooglePlayOutput_cleaned.csv', index=False, sep=';', encoding='utf-8')

    print(df.head())  # This will print the first 5 rows of the DataFrame after cleanup

    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Transform files based on the specified function.')
    parser.add_argument('function_name', help='The name of the function to execute')
    parser.add_argument('input_file', help='The path to the input file')
    parser.add_argument('output_file', help='The path to the output file')

    args = parser.parse_args()

    # Call the appropriate function based on the argument
    if args.function_name == 'transform_GooglePlayData':
        transform_GooglePlayData(args.input_file, args.output_file)
    elif args.function_name == 'transform_AppStoreData':
        transform_AppStoreData(args.input_file, args.output_file)
    # Add more elif statements for additional functions