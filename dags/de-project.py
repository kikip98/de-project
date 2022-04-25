from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
import pandas as pd
import requests
import re
import json
import io
from bs4 import BeautifulSoup
import numpy as np
import logging
from textblob import TextBlob

log = logging.getLogger(__name__)

# =============================================================================
# 1. Set up the main configurations for DAG
# =============================================================================

default_args = {
    'start_date': datetime(2022, 4, 3),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'aws_conn_id':"aws_default",
    'postgres_conn_id':'postgres_conn_id',
    'db_name': Variable.get("schema_postgres", deserialize_json=True)['db_name'],
    'bucket_name': 'airline-project-kikipng' ,
    'bearer_token': Variable.get("bearer_token", deserialize_json=True)['bearer_token'],
    'users_key': Variable.get("parquet_users_key", deserialize_json=True)["key"],
    'tweets_key': Variable.get("parquet_tweets_key", deserialize_json=True)["key"],
    'skytrax_key': Variable.get("parquet_skytrax_key", deserialize_json=True)["key"],
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('airline_project', 
          schedule_interval='@weekly', 
          catchup=False,
          default_args=default_args)

# =============================================================================
# 2. Define functions
# =============================================================================

    
   
def create_schema(**kwargs):

    
    """
	Create postgres schema that twitter data are going to be stored
	"""
    schema_postgres = Variable.get("schema_postgres", deserialize_json=True)
    bucket_name = kwargs['bucket_name']
    key = schema_postgres['key']

    s3 = S3Hook(kwargs['aws_conn_id'])

    log.info('Reading sql file with queries to create database')
    sql_queries = s3.read_key(key, bucket_name)

    # Connect to the database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    log.info('Initialised connection to postgres database') 

    # Execute query
    cursor.execute(sql_queries)
    conn.commit()
    log.info('Creation of schema')

def get_twitter_data(**kwargs):
    
    """
	Scrape tweet data and user data using the Twitter v2 API
	"""
    
    # Create dataframe skeletons
    df_tweets = pd.DataFrame(columns = ["tweet_id", "user_id","airline", 
                                        "tweet_text", "tweet_nb_retweets", "tweet_nb_likes",
                                        "tweet_nb_replies", "tweet_created_at"])

    tweets_row_index = 0
    
    df_users = pd.DataFrame(columns = ["user_id", "user_username","user_name", 
                                        "user_created_at", "user_verified","user_nb_following",
                                        "user_nb_followers", "user_nb_tweets","user_nb_listed"])

    users_row_index = 0
    
    # Get twitter API bearer token
    twitter_bearer_token = kwargs['bearer_token']

    log.info('Bearer token received')

    # Prepare the headers to pass the authentication to Twitter's api
    headers = {'Authorization': 'Bearer {}'.format(twitter_bearer_token)}


    queries =  ["#BritishAirways lang:en", "@BritishAirways lang:en"]
    max_results = 50

    for query in queries:
        
        airline_name = re.sub(r"\B([A-Z])", r" \1", query[1:-8])
        
        params = {'query': query,
                  'max_results': max_results,
                  'expansions': 'author_id',
                  'tweet.fields': 'id,text,author_id,created_at,public_metrics',
                  'user.fields': 'id,name,username,created_at,public_metrics,verified'}

        # Send the request to get the most recent tweets
        response = requests.get('https://api.twitter.com/2/tweets/search/recent', headers=headers, params=params)
        
        
        # Validates that the query was successful
        if response.status_code == 200:

            # Convert the query result to a dictionary that we can iterate over
            tweets =  json.loads(response.text) 
            
            # Store request responses in variables
            for tweet in tweets['data']:
                tweet_id = tweet['id']
                tweet_user_id = tweet['author_id']
                tweet_text = tweet['text']
                tweet_nb_retweets = tweet['public_metrics']['retweet_count']
                tweet_nb_likes = tweet['public_metrics']['like_count']
                tweet_nb_replies  = tweet['public_metrics']['reply_count']
                tweet_created_at = tweet['created_at']

                # Append twitter_row to df_tweets dataframe
                tweets_row = [tweet_id, tweet_user_id, airline_name, tweet_text, int(tweet_nb_retweets), 
                              int(tweet_nb_likes), int(tweet_nb_replies), tweet_created_at]


                df_tweets.loc[tweets_row_index] = tweets_row
                tweets_row_index += 1
                

            for tweet in tweets['includes']['users']:
                user_id = tweet['id']
                user_verified = tweet['verified']   
                user_name = tweet['name']
                user_username = tweet['username']   
                user_created_at = tweet['created_at']   
                user_nb_followers = tweet['public_metrics']['followers_count']
                user_nb_following  = tweet['public_metrics']['following_count']
                user_nb_tweets = tweet['public_metrics']['tweet_count']
                user_nb_listed  = tweet['public_metrics']['listed_count']
                                
                                
                # Append users_row to df_users dataframe
                users_row = [user_id, user_username, user_name, user_created_at, 
                             bool(user_verified), int(user_nb_following), int(user_nb_followers),
                              int(user_nb_tweets), int(user_nb_listed)]


                df_users.loc[users_row_index] = users_row
                users_row_index += 1

    json_tweets = df_tweets.to_json()
    json_users = df_users.to_json()          
    return json_tweets, json_users

def save_twitter_data_to_s3(**kwargs):
    
    """
	Save tweet data and user data to S3 bucket
	"""

    bucket_name = kwargs['bucket_name']
    users_key = kwargs['users_key']
    tweets_key = kwargs['tweets_key']

    s3 = S3Hook(kwargs['aws_conn_id'])

    # Get the task instance
    task_instance = kwargs['ti']

    # Get the output of the previous task
    json_tweets, json_users = task_instance.xcom_pull(
        task_ids="scrape_twitter_data_task")

    log.info('xcom from get_twitter_data:{0}'.format(json_tweets))
    log.info('xcom from get_twitter_data:{0}'.format(json_users))
    
    log.info('Convert scraped data to parquet files')

    # Prepare the files to send to s3 as parquet
    tweets_parquet_buffer = io.BytesIO()
    df_tweets  = pd.read_json(json_tweets)
    df_tweets.to_parquet(tweets_parquet_buffer)

    users_parquet_buffer = io.BytesIO()
    df_users  = pd.read_json(json_users)
    df_users.to_parquet(users_parquet_buffer)

    # Save parquet files to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    tweets_data = tweets_parquet_buffer.getvalue()
    users_data = users_parquet_buffer.getvalue()

    log.info('Saving parquet files to s3')
    tweets_object = s3.Object(bucket_name, tweets_key)
    users_object = s3.Object(bucket_name, users_key)

    # Write the files to S3 bucket in specific path defined in key
    tweets_object.put(Body=tweets_data)
    users_object.put(Body=users_data)

    log.info('Finished saving the scraped twitter data to s3 in parquet files')

def get_skytrax_reviews(**kwargs): 

    # Set a DataFrame and Lists to store information
    reviews_df = pd.DataFrame(columns = [ "airline","overall_rating","review_title","review_author",
                                        "review_text", "review_date_published", "aircraft",
                                        "traveller_type", "cabin_flown", "route", "date_flown",
                                         "value_for_money", "inflight_entertainment",
                                         "ground_service", "seat_comfort", "food_and_beverages", "cabin_staff_service",
                                        "wifi_and_connectivity", "recommendation"])

    reviews_row_index = 0
    
    airline_names = ["British Airways"]

    for airline_name in airline_names:

        # Decide how many pages you want to iterate over
        for page in range(2): 
            # Specify url: urls
            url = "https://www.airlinequality.com/airline-reviews/" + str(airline_name).lower().replace(' ', '-') + "/page/" + str(page) + "/?sortby=post_date%3ADesc&pagesize=50"

            # Package the request, send the request and catch the response
            response = requests.get(url)

            # Create a BeautifulSoup object from the HTML in the variable: soup
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the reviews section in soup
            reviews = soup.find_all("article", {"itemprop" : "review"})

            # Iterate over each review and store relevant information to variables 
            for review in reviews:

                try: 
                    overall_rating = review.find("span", {"itemprop" : "ratingValue"}).getText()
                except:
                    overall_rating = 0

                try:
                    review_title = review.find('h2', {'class': 'text_header'}).getText()
                    review_title = review_title[1:-1]
                except:
                    review_title = "no title"

                try:
                    review_author = review.find("span", {"itemprop" : "name"}).getText()
                except:
                    review_author = "anonymous"

                try:
                    review_text = review.find("div", {"itemprop":"reviewBody" }).getText()
                    review_text = review_text[review_text.find('|'):].replace('|', "").strip(" ")
                except:
                    review_text = "no text"

                try:
                    review_date_published= review.find('time', {"itemprop" : "datePublished"}).getText()
                except:
                    review_date_published = "unknown"

                try:
                    aircraft = review.find("td", {"class": "review-rating-header aircraft"}).find_next().get_text()
                except:
                    aircraft = "unknown"

                try:
                    traveller_type = review.find("td", {"class": "review-rating-header type_of_traveller"}).find_next().get_text()
                except:
                    traveller_type = "unknown"

                try:
                    cabin_flown = review.find("td", {"class": "review-rating-header cabin_flown"}).find_next().get_text()
                except:
                    cabin_flown = "unknown"

                try:
                    route = review.find("td", {"class": "review-rating-header route"}).find_next().get_text()
                except:
                     route = "unknown"

                try:
                    date_flown = review.find("td", {"class": "review-rating-header date_flown"}).find_next().get_text()
                except:
                     date_flown = "unknown"

                try:
                    seat_comfort =len(review.find("td", {"class": str("review-rating-header seat_comfort") }).find_next().find_all(class_="star fill"))
                except:
                    seat_comfort = 0

                try:
                    cabin_staff_service =len(review.find("td", {"class": str("review-rating-header cabin_staff_service") }).find_next().find_all(class_="star fill"))
                except:
                    cabin_staff_service = 0

                try:
                    food_and_beverages =len(review.find("td", {"class": str("review-rating-header food_and_beverages") }).find_next().find_all(class_="star fill"))
                except:
                    food_and_beverages = 0

                try:
                    ground_service =len(review.find("td", {"class": str("review-rating-header ground_service") }).find_next().find_all(class_="star fill"))
                except:
                    ground_service = 0

                try:
                    inflight_entertainment =len(review.find("td", {"class": str("review-rating-header inflight_entertainment") }).find_next().find_all(class_="star fill"))
                except:
                    inflight_entertainment = 0

                try:
                    value_for_money =len(review.find("td", {"class": str("review-rating-header value_for_money") }).find_next().find_all(class_="star fill"))
                except:
                    value_for_money = 0

                try:
                    wifi_and_connectivity =len(review.find("td", {"class": str("review-rating-header wifi_and_connectivity") }).find_next().find_all(class_="star fill"))
                except:
                    wifi_and_connectivity = 0

                try:
                    recommendation = review.find("td", {"class": "review-rating-header recommended"}).find_next().get_text()
                except:
                     recommendation = np.nan

                
                # Append relevant information to DataFrame
                review_info_row = [airline_name, overall_rating, review_title, review_author,
                              review_text, review_date_published, aircraft,
                              traveller_type, cabin_flown, route, date_flown,
                              int(value_for_money), int(inflight_entertainment),
                              int(ground_service), int(seat_comfort), int(food_and_beverages), 
                              int(cabin_staff_service), int(wifi_and_connectivity), recommendation]


                reviews_df.loc[reviews_row_index] = review_info_row
                reviews_row_index += 1
                
    log.info("Finished scraping Skytrax reviews")
    json_skytrax = reviews_df.to_json()          
    return json_skytrax 

def save_skytrax_reviews_to_s3(**kwargs):
    
    """
	Save tweet data and user data to S3 bucket
	"""

    bucket_name = kwargs['bucket_name']
    skytrax_key = kwargs['skytrax_key']

    s3 = S3Hook(kwargs['aws_conn_id'])

    # Get the task instance
    task_instance = kwargs['ti']

    # Get the output of the previous task
    json_skytrax = task_instance.xcom_pull(
        task_ids="scrape_skytrax_reviews_task")

    log.info('xcom from get_skytrax_reviews:{0}'.format(json_skytrax))
    
    log.info('Convert scraped data to parquet files')

    # Prepare the files to send to s3 as parquet

    skytrax_parquet_buffer = io.BytesIO()
    df_skytrax  = pd.read_json(json_skytrax)
    df_skytrax.to_parquet(skytrax_parquet_buffer)
    
    # Save parquet files to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    skytrax_data = skytrax_parquet_buffer.getvalue()

    log.info('Saving parquet file to s3')
    skytrax_object = s3.Object(bucket_name, skytrax_key)

    # Write the files to S3 bucket in specific path defined in key
    skytrax_object.put(Body=skytrax_data)

    log.info('Finished saving the scraped skytrax reviews to s3 in parquet files')

def clean_text(text):
    '''
    Insert text and output cleaned text
    '''  
    text = re.sub("(https://)\S+", "", text) # Remove links
    text = re.sub("(www.)\S+", "", text) # Remove links
    text = re.sub("(\\n)|\n|\r|\t", "", text) # Remove CR, tab, and LR
    text = re.sub("@([A-Za-z0-9_]+)", "", text) # Remove usernames
    text = re.sub("#([A-Za-z0-9_]+)", "", text) # Remove hashtags
    text = re.sub("[0-9]", "", text) # Remove numbers
    text = re.sub("\:|\/|\#|\.|\?|\!|\&|\"|\,|;|^|<|>|\'|\-", " ", text) # Remove special characters
    text = re.sub("[^\x00-\x7F]+", "", text) #Remove emojis
    text = re.sub("RT ", "", text) # Remove RT

    text = text.lower() # Make lowercase   
    stop_words = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself",
            "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who",
            "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been",
            "being", "have", "haven", "has", "had","hadn", "having", "do", "does", "did", "doing", "a", "an", "the",
            "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with",
            "about", "against", "between", "into", "through", "during", "before", "after", "above", "below",
            "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further",
            "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same",
            "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

    text = ' '.join([word for word in text.split() if word not in (stop_words)])
    return text

def clean_aircraft(aircraft):
    aircraft = str(aircraft)
    aircraft = re.sub(r'(^.*A318.*$)', 'A318', aircraft)
    aircraft = re.sub(r'(^.*A319.*$)', 'A319', aircraft)
    aircraft = re.sub(r'(^.*A320.*$)', 'A320', aircraft)
    aircraft = re.sub(r'(^.*A321.*$)', 'A321', aircraft)
    aircraft = re.sub(r'(^.*A350.*$)', 'A350', aircraft)
    aircraft = re.sub(r'(^.*A380.*$)', 'A380', aircraft)
    aircraft = re.sub(r'(^.*737.*$)', 'Boeing 737', aircraft)
    aircraft = re.sub(r'(^.*747.*$)', 'Boeing 747', aircraft)
    aircraft = re.sub(r'(^.*757.*$)', 'Boeing 757', aircraft)
    aircraft = re.sub(r'(^.*767.*$)', 'Boeing 767', aircraft)
    aircraft = re.sub(r'(^.*777.*$)', 'Boeing 777', aircraft)
    aircraft = re.sub(r'(^.*787.*$)', 'Boeing 787', aircraft)
    aircrafts = ["A318", "A319", "A320", "A321", "A350", "A380", "Boeing 737", "Boeing 747", 
                 "Boeing 757", "Boeing 767", "Boeing 777", "Boeing 787"]
    if aircraft not in aircrafts:
        aircraft = "unkown"
    return aircraft

def clean_recommendation(recommendation):
    if recommendation!= 'no' or recommendation!= 'yes':
        recommendation == np.nan
    return recommendation
    
def clean_traveller_type(traveller_type):
    traveller_type_L = ['Couple Leisure','Business','Family Leisure','Solo Leisure'] 
    if traveller_type not in traveller_type_L:
        traveller_type == np.nan
    else:
        traveller_type = traveller_type.split()[0]
    return traveller_type       
        
def clean_cabin_flown(cabin_flown):
    cabin_flown_L = ['Business Class','Economy Class', 'Premium Economy', 'First Class'] 
    if cabin_flown not in cabin_flown_L:
        cabin_flown == np.nan
    elif cabin_flown != 'Premium Economy':
        cabin_flown = cabin_flown.split()[0]
    return cabin_flown

def get_sentiment(text):
    value = TextBlob(text).sentiment[0]
    if value == 0.0:
        return "neutral"
    elif value >= 0.0:
        return "positive"
    else:
        return "negative"
    

def import_scraped_data_from_s3_to_pg_db(**kwargs):
    
    '''
    Transfer twitter data from s3 to postgres database
    '''
    
    # Access bucket
    bucket_name = kwargs['bucket_name']
    tweets_key = kwargs['tweets_key']
    users_key = kwargs['users_key']
    skytrax_key = kwargs['skytrax_key']

    s3 = S3Hook(kwargs['aws_conn_id'])
    
    log.info("Established connection to S3 bucket")

    # Get the task instance
    task_instance = kwargs['ti']

   # Access bucket
    s3 = s3.get_resource_type('s3')
    tweets_response = s3.Object(bucket_name, tweets_key).get()
    tweets_object = tweets_response['Body'].read()
    
    users_response = s3.Object(bucket_name, users_key).get()
    users_object = users_response['Body'].read()

    skytrax_response = s3.Object(bucket_name, skytrax_key).get()
    skytrax_object = skytrax_response['Body'].read()

    # Convert parquet files into pandas dataframe
    df_tweets = pd.read_parquet(io.BytesIO(tweets_object))
    df_users = pd.read_parquet(io.BytesIO(users_object))
    df_skytrax = pd.read_parquet(io.BytesIO(skytrax_object))

    # Clean text of tweet
    df_tweets['tweet_text'] =  df_tweets['tweet_text'].map(lambda x: clean_text(x))

    # Remove emojis from names at twitter
    df_users["user_name"] =  [re.sub("[^\x00-\x7F]+", "",  str(x)).strip() for x in df_users["user_name"]]

    # Clean text of review title and review body and clean aircraft
    df_skytrax['review_title'] =  df_skytrax['review_title'].map(lambda x: clean_text(x))
    df_skytrax['review_text'] =  df_skytrax['review_text'].map(lambda x: clean_text(x))
    df_skytrax['review_sentiment'] =  df_skytrax['review_text'].map(lambda x: get_sentiment(x))

    df_skytrax['aircraft'] =  df_skytrax['aircraft'].map(lambda x: clean_aircraft(x))
    df_skytrax['traveller_type'] =  df_skytrax['traveller_type'].map(lambda x: clean_traveller_type(x))
    df_skytrax['cabin_flown'] =  df_skytrax['cabin_flown'].map(lambda x: clean_cabin_flown(x))
    df_skytrax['recommendation'] =  df_skytrax['recommendation'].map(lambda x: clean_recommendation(x))
    df_skytrax['date_flown'] = pd.to_datetime(df_skytrax['date_flown']).dt.strftime('%m-%Y')
    df_skytrax['review_date_published'] = pd.to_datetime(df_skytrax['review_date_published']).dt.strftime('%d-%m-%Y')
    df_skytrax["date_flown"] = np.where(df_skytrax["review_date_published"] < df_skytrax["date_flown"], np.nan, df_skytrax["date_flown"])
    df_skytrax = df_skytrax.drop(columns=['review_text']) 

    log.info("Retrieved and cleaned data from S3 bucket")

    # Connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection to postgres database')
    
    log.info('Loading data row by row into the database')
    
    # Insert users' data to twitter_users table
    s1 = """INSERT INTO airline_postgres_schema.twitter_users(user_id,user_username,user_name,user_created_at,
    user_verified,user_nb_following,user_nb_followers, user_nb_tweets, user_nb_listed) VALUES (%s, %s, %s, %s, %s, 
    %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET (user_id,user_username,user_name,user_created_at,
    user_verified,user_nb_following,user_nb_followers, user_nb_tweets, user_nb_listed)=(EXCLUDED.user_id,
    EXCLUDED.user_username,EXCLUDED.user_name,EXCLUDED.user_created_at, EXCLUDED.user_verified,
    EXCLUDED.user_nb_following,EXCLUDED.user_nb_followers, EXCLUDED.user_nb_tweets, EXCLUDED.user_nb_listed);"""
    
    cursor.executemany(s1, df_users.values.tolist())
    conn.commit()

    log.info('Finished importing users\' data to postgres database')    

    # Insert tweets' data to twitter_users table
    s2 = """INSERT INTO airline_postgres_schema.twitter_tweets(tweet_id,user_id,airline, tweet_text,
    tweet_nb_retweets,tweet_nb_likes,tweet_nb_replies,tweet_created_at) VALUES (%s, %s, %s, %s, %s, %s, 
    %s, %s) ON CONFLICT (tweet_id) DO UPDATE SET (tweet_id,user_id,airline, tweet_text,tweet_nb_retweets,
    tweet_nb_likes,tweet_nb_replies,tweet_created_at)=(EXCLUDED.tweet_id,EXCLUDED.user_id,EXCLUDED.airline, 
    EXCLUDED.tweet_text,EXCLUDED.tweet_nb_retweets,EXCLUDED.tweet_nb_likes,EXCLUDED.tweet_nb_replies,EXCLUDED.tweet_created_at);"""
    cursor.executemany(s2, df_tweets.values.tolist())
    conn.commit()

    log.info('Finished importing the scraped tweets\' to postgres database')

    # Insert skytrax reviews to skytrax_reviews table
    s3 = """INSERT INTO airline_postgres_schema.skytrax_reviews(review_id, airline, overall_rating, review_title, review_author, 
    review_date_published, aircraft, traveller_type, cabin_flown, route, date_flown, value_for_money, 
    inflight_entertainment, ground_service, seat_comfort, food_and_beverages, cabin_staff_service, wifi_and_connectivity, 
    recommendation,review_sentiment) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (review_title) DO UPDATE SET
    (review_id, airline, overall_rating, review_title, review_author, 
    review_date_published, aircraft, traveller_type, cabin_flown, route, date_flown, value_for_money, 
    inflight_entertainment, ground_service, seat_comfort, food_and_beverages, cabin_staff_service, wifi_and_connectivity, 
    recommendation,review_sentiment)=(EXCLUDED.review_id, EXCLUDED.airline, EXCLUDED.overall_rating, EXCLUDED.review_title, EXCLUDED.review_author, 
    EXCLUDED.review_date_published, EXCLUDED.aircraft, EXCLUDED.traveller_type, EXCLUDED.cabin_flown, EXCLUDED.route, EXCLUDED.date_flown, EXCLUDED.value_for_money, 
    EXCLUDED.inflight_entertainment, EXCLUDED.ground_service, EXCLUDED.seat_comfort, EXCLUDED.food_and_beverages, EXCLUDED.cabin_staff_service, EXCLUDED.wifi_and_connectivity, 
    EXCLUDED.recommendation, EXCLUDED.review_sentiment);"""
    cursor.executemany(s3, df_skytrax.values.tolist())
    conn.commit()
    
    log.info('Finished importing the scraped skytrax reviews to postgres database')


# =============================================================================
# 3. Create Operators
# =============================================================================


create_schema_task = PythonOperator(
    task_id='create_schema_task',
    provide_context=True,
    python_callable=create_schema,
    op_kwargs=default_args,
    dag=dag,
)

scrape_twitter_data_task = PythonOperator(
    task_id='scrape_twitter_data_task',
    provide_context=True,
    python_callable=get_twitter_data,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,
)

scrape_skytrax_reviews_task = PythonOperator(
    task_id='scrape_skytrax_reviews_task',
    provide_context=True,
    python_callable=get_skytrax_reviews,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,
)

save_twitter_data_to_s3_task = PythonOperator(
    task_id='save_twitter_data_to_s3_task',
    provide_context=True,
    python_callable=save_twitter_data_to_s3,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,
)

save_skytrax_reviews_to_s3_task = PythonOperator(
    task_id='save_skytrax_reviews_to_s3_task',
    provide_context=True,
    python_callable=save_skytrax_reviews_to_s3,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,
)

import_scraped_data_from_s3_to_pg_db_task = PythonOperator(
    task_id='import_scraped_data_from_s3_to_pg_db_task',
    provide_context=True,
    python_callable=import_scraped_data_from_s3_to_pg_db,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,
)


# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================

create_schema_task >> scrape_twitter_data_task >>  scrape_skytrax_reviews_task >> save_twitter_data_to_s3_task >> save_skytrax_reviews_to_s3_task >> import_scraped_data_from_s3_to_pg_db_task 
