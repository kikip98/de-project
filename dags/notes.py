def import_twitter_data_from_s3_to_pg_db(**kwargs):
    
    '''
    Transfer twitter data from s3 to postgres database
    '''
    
    # Access bucket
    bucket_name = kwargs['bucket_name']
    tweets_key = kwargs['tweets_key']
    users_key = kwargs['users_key']

    s3 = S3Hook(kwargs['aws_conn_id'])
    
    log.info("Established connection to S3 bucket")

    # Get the task instance
    task_instance = kwargs['ti']

    # Read the content of the key from the bucket
    tweets_bytes = s3.read_key(tweets_key, bucket_name)
    users_bytes = s3.read_key(users_key, bucket_name)

    # Read the CSV
    clean_tweets = pd.read_parquet(io.BytesIO(tweets_bytes))#, encoding='utf-8')
    
    log.info('Passing tweets data from S3 bucket')
    
    clean_users = pd.read_parquet(io.BytesIO(users_bytes))
    
    log.info('Passing users data from S3 bucket')
    

    # Connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection to postgres database')
    
    log.info('Loading data row by row into the database')

    # Insert tweets' data to twitter_users table
    s2 = """INSERT INTO airline_schema.twitter_tweets(tweet_id,user_id,airline, tweet_text,tweet_nb_retweets,
    tweet_nb_likes,tweet_nb_replies,tweet_created_at) VALUES (%s, %s, %s, %s,%s,%s,%s,%s));"""
    cursor.executemany(s2, df_tweets)
    conn.commit()

    log.info('Finished importing the scraped tweets\' to postgres database')
    
    # Insert users' data to twitter_users table
    s1 = """INSERT INTO airline_schema.twitter_users(user_id,user_username,user_name,user_created_at,
    user_verified,user_nb_following,user_nb_followers, user_nb_tweets, user_nb_listed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
    
    cursor.executemany(s1, df_users)
    conn.commit()

    log.info('Finished importing users\' data to postgres database')    



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
    print(sql_queries)

    # Connect to the database
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    log.info('Initialised connection to postgres database')

    # Execute query
    cursor.execute(sql_queries)
    conn.commit()
    log.info('Creation of schema')



def create_schema(**kwargs):

    
def create_schema(**kwargs):


def save_skytrax_reviews_to_s3(**kwargs):

    ###################


    bucket_name = kwargs['bucket_name']
    skytrax_key_B = Variable.get("parquet_skytrax_key_B", deserialize_json=True)["key"]
    s3 = S3Hook(kwargs['aws_conn_id'])

    # Get the task instance
    task_instance = kwargs['ti']

    # Get the output of the bash task
    json_skytrax = task_instance.xcom_pull(
        task_ids="scrape_skytrax_reviews_task")

    # Load the list of dictionaries with the scraped data from the previous task into a pandas dataframe
    log.info('Loading scraped data into pandas dataframe')
    df = pd.read_json(json_skytrax)

    log.info('Saving scraped data to {0}'.format(skytrax_key_B))

    # Prepare the file to send to s3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Save the pandas dataframe as a csv to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    data = csv_buffer.getvalue()

    print("Saving CSV file")
    object = s3.Object(bucket_name, skytrax_key_B)

    # Write the file to S3 bucket in specific path defined in key
    object.put(Body=data)

    log.info('Finished saving the scraped data to s3')


    
        
    """
	Create postgres schema that twitter data are going to be stored
	"""
    
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    log.info('Initialised connection to postgres database')
    sql_queries = """

    CREATE SCHEMA IF NOT EXISTS airline_postgres_schema;
    
    CREATE TABLE IF NOT EXISTS airline_postgres_schema.twitter_users(
    "user_id" varchar PRIMARY KEY,
    "user_username" varchar,
    "user_name" varchar,
    "user_created_at" varchar,
    "user_verified" boolean,
    "user_nb_following" int,
    "user_nb_followers" int,
    "user_nb_tweets" int,
    "user_nb_listed" int
    );  

    CREATE TABLE IF NOT EXISTS airline_postgres_schema.twitter_tweets(
    "tweet_id" varchar PRIMARY KEY,
    "user_id" varchar,
    "airline" varchar,
    "tweet_text" varchar,
    "tweet_nb_retweets" int,
    "tweet_nb_likes" int,
    "tweet_nb_replies" int,
    "tweet_created_at" varchar
    );
    
    ALTER TABLE airline_postgres_schema.twitter_tweets ADD FOREIGN KEY (user_id) REFERENCES airline_postgres_schema.twitter_users (user_id);
    
    
    CREATE TABLE IF NOT EXISTS airline_postgres_schema.skytrax_reviews(
    "review_id"  int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    "airline" varchar,
    "overall_rating" int,
    "review_title" varchar,
    "review_author" varchar,
    "review_text" varchar unique,
    "review_date_published" varchar,
    "aircraft" varchar,
    "traveller_type" varchar,
    "cabin_flown" varchar,
    "route" varchar,
    "date_flown" varchar,
    "value_for_money" int,
    "inflight_entertainment" int,
    "ground_service" int,
    "seat_comfort" int,
    "food_and_beverages" int,
    "cabin_staff_service" int,
    "wifi_and_connectivity" int,
    "recommendation" varchar
    );  
    """

    # Execute query
    cursor.execute(sql_queries)
    conn.commit()
    log.info('Creation of schema')