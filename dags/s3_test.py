"""
S3 Sensor Connection Test
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 1),
    'email': ['something@here.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_dag_test', default_args=default_args, schedule_interval= '@once')

# t1 = BashOperator(
#     task_id='bash_test',
#     bash_command='echo "hello, it should work" > s3_conn_test.txt',
#     dag=dag)

# sensor = S3KeySensor(
#     task_id='check_s3_for_file_in_s3',
#     bucket_key='airline-project-kikipng',
#     wildcard_match=True,
#     bucket_name='airline-project-kikipng',
#     s3_conn_id='airline-project-kikipng',
#     timeout=18*60*60,
#     poke_interval=120,
#     dag=dag)

# t1.set_upstream(sensor)
    
def get_twitter_data():
    
    """
	Scrape tweet data and user data using the Twitter v2 API
	"""
    
    # Import packages  
    import pandas as pd
    import requests
    import re
    import json
    
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
    twitter_bearer_token = 'AAAAAAAAAAAAAAAAAAAAAO17awEAAAAAA5JntFFt2SCChJwK2UWU1tKD43c%3DK2oX1tBb6pQjf8booRY6zNL8mzBmZW9b2zBtnEDUHKItNBV4TW'


    # Prepare the headers to pass the authentication to Twitter's api
    headers = {'Authorization': 'Bearer {}'.format(twitter_bearer_token)}


    queries =  ["#BritishAirways lang:en", "#Ryanair lang:en"]
    max_results = 100

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
                tweet_user_id =  '2333'
                tweet_text = tweet['text']
                tweet_text = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", tweet_text) #Remove http links
                tweet_text = re.sub(r"[^\x00-\x7F]+", "", tweet_text) #Remove emojis
                tweet_text = re.sub(r"(#|@|\n)", "", tweet_text) #Remove hashtag, @, _, \n but keep the text
                tweet_text = " ".join(tweet_text.split())
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
            

    df_tweets.to_csv('/opt/airflow/df_tweets.csv')		
    df_users.to_csv('/opt/airflow/df_users.csv')
    


def convert_csv_to_parquet():
    import pandas as pd
    df_tweets = pd.read_csv('/opt/airflow/data/df_tweets.csv')
    df_users = pd.read_csv('/opt/airflow/data/df_users.csv')

    df_tweets.to_parquet('/opt/airflow/data/parquet_tweets.parquet')
    df_users.to_parquet('/opt/airflow/data/parquet_users.parquet')



def save_tweets_to_s3():
    import boto3

    #Creating Session With Boto3.
    session = boto3.Session(
    aws_access_key_id='AKIA2X2ER6BK4N7IZCVI',
    aws_secret_access_key='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'
    )

    #Creating S3 Resource From the Session.
    s3 = session.resource('s3')
    result = s3.Bucket('airline-project-kikipng').upload_file('/opt/airflow/data/parquet_tweets.parquet','parquet_tweets.parquet')

    print(result)


def save_users_to_s3():
    import boto3

    #Creating Session With Boto3.
    session = boto3.Session(
    aws_access_key_id='AKIA2X2ER6BK4N7IZCVI',
    aws_secret_access_key='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'
    )

    #Creating S3 Resource From the Session.
    s3 = session.resource('s3')
    result = s3.Bucket('airline-project-kikipng').upload_file('/opt/airflow/data/parquet_users.parquet','parquet_users.parquet')

    print(result)


from airflow.operators.python_operator import PythonOperator

get_twitter_data_task = PythonOperator(
    task_id='get_twitter_data_task',
    provide_context=True,
    python_callable=get_twitter_data,
    dag=dag,
)


convert_csv_to_parquet_task = PythonOperator(
    task_id='convert_csv_to_parquet_task',
    provide_context=True,
    python_callable=convert_csv_to_parquet,
    dag=dag,
)

tweets_to_s3_task = PythonOperator(
    task_id='tweets_to_s3_task',
    provide_context=True,
    python_callable=save_tweets_to_s3,
    dag=dag,
)

users_to_s3_task = PythonOperator(
    task_id='users_to_s3_task',
    provide_context=True,
    python_callable=save_users_to_s3,
    dag=dag,
)

get_twitter_data_task >> convert_csv_to_parquet_task >> tweets_to_s3_task >> users_to_s3_task