import pandas as pd
from textblob import TextBlob
from ast import literal_eval
import re
from datetime import datetime, timezone
import json
import argparse
from iso639 import languages

# Example transformation function
def transform_AppStoreData(input_file, output_file):

    df = pd.read_csv('./AppStoreOutput.csv', delimiter=',', encoding='utf-8')
    df['released'] = pd.to_datetime(df['released'])
    df['updated'] = pd.to_datetime(df['updated'])
    df['score'] = pd.to_numeric(df['score'], errors='coerce')
    df['free'] = df['free'].astype(int)
    df['supports_iPhone'] = 0
    df['supports_iPad'] = 0
    df['supports_Mac'] = 0
    df['days_since_last_update'] = (datetime.now(timezone.utc) - df['updated']).dt.days
    df['app_age'] = (df['updated'] - df['released']).dt.days

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

    languages_exploded = df[['appId', 'languages']].explode('languages')
    genres_exploded = df[['appId', 'genres']].explode('genres')


    # Expanded Language-to-Countries Mapping
    language_to_countries = {
        'AF': ['Afghanistan'],
        'AM': ['Armenia'],
        'AN': ['Netherlands Antilles'],
        'AR': ['Saudi Arabia', 'Iraq', 'Egypt', 'Algeria', 'Morocco', 'Sudan', 'Yemen', 'Syria', 'Tunisia', 'Jordan', 'Libya', 'Lebanon', 'Oman', 'Kuwait', 'Mauritania', 'Qatar', 'Bahrain', 'United Arab Emirates'],
        'AZ': ['Azerbaijan'],
        'BE': ['Belarus'],
        'BG': ['Bulgaria'],
        'BN': ['Bangladesh', 'India'],
        'BR': ['Brazil'],
        'BS': ['Bosnia and Herzegovina'],
        'CA': ['Spain', 'Andorra'],
        'CO': ['France'],
        'CS': ['Czech Republic', 'Slovakia'],
        'CY': ['Wales'],
        'DA': ['Denmark', 'Greenland', 'Faroe Islands'],
        'DE': ['Germany', 'Austria', 'Switzerland', 'Luxembourg', 'Liechtenstein'],
        'EL': ['Greece', 'Cyprus'],
        'EN': ['United States', 'United Kingdom', 'Canada', 'Australia', 'Ireland', 'New Zealand', 'South Africa'],
        'EO': ['Worldwide'],  # Esperanto is a constructed international auxiliary language.
        'ES': ['Spain', 'Mexico', 'Colombia', 'Argentina', 'Peru', 'Venezuela', 'Chile', 'Ecuador', 'Guatemala', 'Cuba', 'Bolivia', 'Dominican Republic', 'Honduras', 'Paraguay', 'El Salvador', 'Nicaragua', 'Costa Rica', 'Puerto Rico', 'Panama', 'Uruguay'],
        'ET': ['Estonia'],
        'EU': ['Spain'],  # Basque Country
        'FA': ['Iran', 'Afghanistan', 'Tajikistan'],
        'FI': ['Finland', 'Sweden'],
        'FR': ['France', 'Canada', 'Belgium', 'Switzerland', 'Luxembourg', 'Monaco', 'Congo', 'Ivory Coast', 'Madagascar', 'Cameroon', 'Burkina Faso', 'Niger', 'Senegal', 'Mali', 'Rwanda', 'Belgium', 'Guinea'],
        'FY': ['Netherlands'],
        'GA': ['Ireland'],
        'GD': ['Scotland'],
        'GL': ['Spain'],
        'GU': ['India'],
        'HE': ['Israel'],
        'HI': ['India'],
        'HR': ['Croatia', 'Bosnia and Herzegovina'],
        'HT': ['Haiti'],
        'HU': ['Hungary'],
        'HY': ['Armenia', 'Nagorno-Karabakh Republic'],
        'IA': ['Worldwide'],  # Interlingua is a constructed international auxiliary language.
        'ID': ['Indonesia'],
        'IG': ['Nigeria'],
        'IS': ['Iceland'],
        'IT': ['Italy', 'Switzerland', 'San Marino', 'Vatican City'],
        'JA': ['Japan'],
        'KA': ['Georgia'],
        'KK': ['Kazakhstan'],
        'KM': ['Cambodia'],
        'KN': ['India'],
        'KO': ['South Korea', 'North Korea'],
        'KU': ['Turkey', 'Iraq', 'Iran', 'Syria'],
        'KY': ['Kyrgyzstan'],
        'LO': ['Laos'],
        'LT': ['Lithuania'],
        'LV': ['Latvia'],
        'MK': ['North Macedonia'],
        'ML': ['India', 'Sri Lanka'],
        'MN': ['Mongolia'],
        'MR': ['India'],
        'MS': ['Malaysia', 'Brunei', 'Singapore'],
        'MT': ['Malta'],
        'MY': ['Myanmar'],
        'NB': ['Norway'],
        'NE': ['Niger'],
        'NL': ['Netherlands', 'Belgium', 'Suriname'],
        'NN': ['Norway'],
        'OC': ['France'],
        'PA': ['India', 'Pakistan'],
        'PL': ['Poland'],
        'PS': ['Afghanistan', 'Pakistan'],
        'PT': ['Portugal', 'Brazil', 'Angola', 'Mozambique', 'Cape Verde', 'Guinea-Bissau', 'São Tomé and Príncipe', 'East Timor'],
        'RO': ['Romania', 'Moldova'],
        'RU': ['Russia', 'Belarus', 'Kazakhstan', 'Kyrgyzstan'],
        'SC': ['Italy'],
        'SE': ['Sweden'],
        'SI': ['Sri Lanka'],
        'SK': ['Slovakia'],
        'SL': ['Slovenia'],
        'SN': ['Zimbabwe'],
        'SQ': ['Albania', 'Kosovo'],
        'SR': ['Serbia', 'Bosnia and Herzegovina', 'Montenegro', 'Kosovo'],
        'SV': ['Sweden'],
        'SW': ['Tanzania', 'Kenya', 'Uganda'],
        'TA': ['India', 'Sri Lanka'],
        'TE': ['India'],
        'TG': ['Tajikistan'],
        'TH': ['Thailand'],
        'TL': ['Timor-Leste'],
        'TR': ['Turkey', 'Cyprus'],
        'TT': ['Russia'],
        'UK': ['Ukraine'],
        'UR': ['Pakistan', 'India'],
        'UZ': ['Uzbekistan'],
        'VI': ['Vietnam'],
        'XH': ['South Africa'],
        'YI': ['Worldwide'],  # Yiddish is spoken by Jewish communities worldwide.
        'YO': ['Nigeria', 'Benin'],
        'ZH': ['China', 'Taiwan', 'Singapore', 'Malaysia'],
        'ZU': ['South Africa'],
    }



    # Map language codes to countries
    languages_exploded['Countries'] = languages_exploded['languages'].map(lambda x: ', '.join(language_to_countries.get(x, ['Unknown'])))
    languages_exploded['Countries'] = languages_exploded['Countries'].fillna('Unknown')
    # Split the 'Countries' column into a list of countries
    languages_exploded['Countries'] = languages_exploded['Countries'].str.split(', ')
    # Explode the 'Countries' column
    languages_exploded = languages_exploded.explode('Countries')

    def get_language_name(lang_code):
        # Convert the language code to lowercase to match the iso639 library's expected format
        lang_code_lower = lang_code.lower()
        try:
            # Attempt to get the language name using the ISO 639-1 code
            lang_name = languages.get(part1=lang_code_lower).name
        except KeyError:
            try:
                # If the ISO 639-1 code lookup fails, try the ISO 639-2/T code
                lang_name = languages.get(part2t=lang_code_lower).name
            except KeyError:
                try:
                    # If the ISO 639-2/T code lookup also fails, try the ISO 639-2/B code
                    lang_name = languages.get(part2b=lang_code_lower).name
                except KeyError:
                    # If none of the lookups are successful, return the original code
                    lang_name = lang_code  # Keeping the original case for visibility
        return lang_name

    # Apply the function to translate language codes to names
    languages_exploded['language_name'] = languages_exploded['languages'].apply(get_language_name)



    # Categorizing Numerical Data
    bins = [0, 50000000, 200000000, float('inf')]
    labels = ['Small', 'Medium', 'Large']


    # Function to convert binary values to 'free' or 'paid'
    def free_convert_to_category(value):
        if value == 1:
            return 'free'
        else:
            return 'paid'

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
        if days_since_last_update <= 30:
            return 'Very Recent Updates'
        elif days_since_last_update <= 90:
            return 'Recently Updated'
        elif days_since_last_update <= 180:
            return 'Moderately Updated'
        elif days_since_last_update <= 365:
            return 'Rarely Updated'
        else:
            return 'Stale'





    df['update_frequency'] = df['days_since_last_update'].apply(categorize_update_frequency)

    ## Apply the categorization function to the 'price' column
    df['price_category'] = df['price'].apply(lambda price: categorize_price(price))

    ## Apply the categorization function to the 'app_age' column
    df['app_age_category'] = df['app_age'].apply(lambda days: categorize_app_age(days))

    ## Apply the categorization function to the 'file_size' column
    df['file_size_category'] = pd.cut(df['size'], bins=bins, labels=labels)


    # Aggregate Languages and Genres
    language_counts = df.filter(regex='^lang_').sum().reset_index()
    genre_counts = df.filter(regex='^_').sum().reset_index()

    # Remove '_' prefix and rename columns
    genre_counts['index'] = genre_counts['index'].str.replace('^_', '', regex=True)
    genre_counts.columns = ['Genre', 'Number of Apps']

    # Remove 'lang_' prefix and rename columns
    language_counts['index'] = language_counts['index'].str.replace('lang_', '', regex=True)
    language_counts.columns = ['Language', 'Number of Apps']


    # Creating a relational table for device support
    device_support = df.melt(id_vars=['appId'], value_vars=['supports_iPhone', 'supports_iPad', 'supports_Mac'], var_name='Device', value_name='Supported')
    device_support = device_support[device_support['Supported'] == 1].drop('Supported', axis=1)
    device_support.to_csv('AppStore_Device_Support.csv', index=False)


    # Apply the function to the 'free' column
    df['free'] = df['free'].apply(free_convert_to_category)


    # Rename columns for clarity
    language_counts.columns = ['Language', 'Number of Apps']
    genre_counts.columns = ['Genre', 'Number of Apps']

    # Preview the aggregated language data
    print(language_counts.head())
    print('\n')
    # Preview the aggregated genre data
    print(genre_counts.head())

    # Device support aggregation
    device_support_counts = df[['supports_iPhone', 'supports_iPad', 'supports_Mac']].sum().reset_index()

    # Rename columns for clarity
    device_support_counts.columns = ['Device Type', 'Number of Apps']

    # Convert device type names to more readable format if necessary
    # Example: You can manually rename each type for clarity
    device_support_counts['Device Type'] = device_support_counts['Device Type'].replace({
        'supports_iPhone': 'iPhone',
        'supports_iPad': 'iPad',
        'supports_Mac': 'Mac'
    })

    # Preview the device support data
    print(device_support_counts)




    # Final DataFrame Cleanup and Saving the Cleaned Data
    columns_to_remove = [
        'id', '', 'description', 'icon', 'genreIds', 'primaryGenreId',
        'requiredOsVersion', 'releaseNotes', 'version', 'developerid', 'developerUrl',
        'screenshots', 'ipadScreenshots', 'appletvScreenshots',
        'languages', 'genres', 'supportedDevices', 'currency', 'developerId', 'reviews', 
    ]
    df.drop(columns_to_remove, axis=1, inplace=True, errors='ignore')

    # Save the modified DataFrame to a new CSV file
    df.to_csv('AppStoreOutput_cleaned.csv', index=False, sep=',', encoding='utf-8')
    languages_exploded.to_csv('AppStore_Languages.csv', index=False)
    genres_exploded.to_csv('AppStore_Genres.csv', index=False)

    pass

def transform_GooglePlayData(input_file, output_file):

    # Load and transform data
    df = pd.read_csv('./GooglePlayOutput.csv', delimiter=',', encoding='utf-8')
    df['released'] = pd.to_datetime(df['released']).dt.tz_localize('UTC')
    df['updated'] = pd.to_datetime(df['updated'], unit='ms', utc=True)
    df['days_since_last_update'] = (datetime.now(timezone.utc) - df['updated']).dt.days

    # Clean data
    df['contentRating'] = df['contentRating'].str.replace('Rated for', '', regex=False).str.strip()
    df['score'] = pd.to_numeric(df['score'], errors='coerce')
    df['free'] = df['free'].astype(int)



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
        if days_since_last_update <= 30:
            return 'Very Recent Updates'
        elif days_since_last_update <= 90:
            return 'Recently Updated'
        elif days_since_last_update <= 180:
            return 'Moderately Updated'
        elif days_since_last_update <= 365:
            return 'Rarely Updated'
        else:
            return 'Stale'




    ## Category parsing
    def parse_json_categories(row):
        try:
            categories_list = json.loads(row)
            # Extract just the names from each category
            return [category['name'] for category in categories_list]
        except:
            return []  # Return an empty list if parsing fails or if row is empty

    df['categories'] = df['categories'].apply(parse_json_categories)


    categories_exploded = df[['appId', 'categories']].explode('categories')




    # Function to convert binary values to 'free' or 'paid'
    def free_convert_to_category(value):
        if value == 1:
            return 'free'
        else:
            return 'paid'



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



    #Calculations 
    df['app_age'] = (df['updated'] - df['released']).dt.days
    df['rating_ratio'] = (df['4*'] + df['5*']) / (df['1*'] + df['2*'])
    df['engagement_score'] = (df['score'] * df['ratings']) / df['minInstalls']
    df['install_to_rating'] = df['minInstalls'] / (df['ratings'] + 1e-10)

    # Assuming IAPRange is a string like "$1.99 - $99.99"
    df[['CurrencySymbol', 'IAPMin', 'CurrencySymbol2', 'IAPMax']] = df['IAPRange'].str.extract(r'([^\d]+)(\d+\.?\d*) - ([^\d]+)(\d+\.?\d*)')

    # Optionally, verify that CurrencySymbol and CurrencySymbol2 match if necessary

    # Convert the extracted strings for min and max prices to numeric values
    df['IAPMin'] = pd.to_numeric(df['IAPMin'], errors='coerce')
    df['IAPMax'] = pd.to_numeric(df['IAPMax'], errors='coerce')

    # You can drop the currency symbol columns if they're not needed, or keep one if you plan to use it
    df.drop(['CurrencySymbol', 'CurrencySymbol2'], axis=1, inplace=True)



    ## Categorizations
    df['update_frequency'] = df['days_since_last_update'].apply(categorize_update_frequency)
    df['app_age_category'] = df['app_age'].apply(lambda days: categorize_app_age(days))
    df['rating_ratio_category'] = df['rating_ratio'].apply(lambda ratio: categorize_rating_ratio(ratio))
    percentiles = df['engagement_score'].quantile([0.25, 0.5, 0.75, 0.9]).to_dict()
    df['engagement_score_category'] = df['engagement_score'].apply(lambda x: categorize_engagement_score(x, percentiles))
    df['price_category'] = df['price'].apply(lambda price: categorize_price(price))
    df['install_to_rating_category'] = df['install_to_rating'].apply(categorize_install_to_rating_ratio)
    df['free'] = df['free'].apply(free_convert_to_category)

    # Clean-up and Output
    columns_to_remove = [
        'description', 'descriptionHTML', 'summary', 'installs', 'maxInstalls', 'scoreText', 'reviews', 'histogram', 'currency', 'androidVersion', 'androidVersionText',
        'androidMaxVersion', 'previewVideo', 'developerId', 'developerEmail', 'developerAddress', 'privacyPolicy', 'developerInternalID', 'genreId', 'icon', 'headerImage',
        'screenshots', 'video', 'videoImage','contentRatingDescription','version', 'recentChanges', 'comments', 'originalPrice', 'discountEndDate', 'categories',  'priceText',
        '1*', '2*', '3*', '4*', '5*', 
    ]
    df.drop(columns_to_remove, axis=1, inplace=True, errors='ignore')
    df['updated'] = df['updated'].dt.strftime('%Y-%m-%d')
    df['released'] = df['released'].dt.strftime('%Y-%m-%d')
    df.to_csv('GooglePlayOutput_cleaned.csv', index=False, sep=',', encoding='utf-8')
    categories_exploded.to_csv('GooglePlay_Categories.csv', index=False)



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