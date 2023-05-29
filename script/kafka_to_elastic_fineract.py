import os
import json
import datetime
import base64
import decimal
import signal

from time import sleep
from dotenv import load_dotenv

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError

import redis

import requests

import smtplib
from email.mime.text import MIMEText

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
from pyspark.sql.functions import col, avg, count, current_date, date_sub

from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.linalg import Vectors

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.16.2,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.13.0 pyspark-shell"

# create a Spark session
spark = SparkSession.builder.appName("kafka_elastic_ia_fineract").getOrCreate()

# Load environment variables from .env file
load_dotenv()

# Access the username and password
username = os.environ.get("USERNAME")
password = os.environ.get("PASSWORD")

# Create a Redis connection
redis_host = os.environ.get("redis_host")
redis_port = os.environ.get("redis_port")
redis_db_tr = os.environ.get("redis_db_tr")
redis_db_acc = os.environ.get("redis_db_acc")

redis_client_tr = redis.Redis(host=redis_host, port=redis_port, db= redis_db_tr)
redis_client_acc = redis.Redis(host=redis_host, port=redis_port, db= redis_db_acc)

# Define the Elasticsearch index name
es_index = os.environ.get("es_index")
es_port = int (os.environ.get("es_port"))
es_host = os.environ.get("es_host")

# Create an Elasticsearch client
es = Elasticsearch(
    [{"host": es_host, "port": es_port, "scheme": "http"}],
    basic_auth=(username, password),
)

# mllib ia model
lr_model = LogisticRegressionModel.load("lr_ml")

def cleanup():
    # Stop the writeStream operation
    try:
        query_account.stop()
        query_transaction.stop()
    except Exception as e:
        print("Error occurred while stopping the query:\n", str(e))
    finally:
        # Stop the SparkSession
        spark.stop()

    # Stop the SparkSession and perform cleanup before exiting
    #spark.stop()
    print("Cleaning up resources...")

# Define a signal handler function
def signal_handler(signal, frame):
    # Cleaning up resources...
    cleanup()

    print('Exiting gracefully...')
    # Perform any cleanup or specific actions here
    exit(0)

# Set the signal handler
signal.signal(signal.SIGINT, signal_handler)

# create a Kafka stream for transaction
df_transaction_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "dbserver1.fineract_default.m_savings_account_transaction")
    .option("group.id", "1")
    .load()
)

# create a Kafka stream for account
df_account_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "dbserver1.fineract_default.m_savings_account")
    .load()
)

# Define the schema for the DataFrame
schema_transaction = StructType(
    [
        StructField("account-id", LongType(), True),
        StructField("amount", DoubleType(), True),
        StructField("customer-id", LongType(), True),
        StructField("datetime", StringType(), True),
        StructField("is_fraud", StringType(), True),
        StructField("transaction-id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("@timestamp", StringType(), True),
    ]
)

# Define the schema for the DataFrame
schema_account = StructType([
    StructField("customer_id",  LongType(), True),
    StructField("account_id", LongType(), True)
])

account_df = spark.createDataFrame([(-1,-1),], schema=schema_account)

dic_trans = {}

# Create a mapping for the Elasticsearch index
# Define the index mapping
mapping = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "account-id": {"type": "long"},
            "amount": {"type": "double"},
            "customer-id": {"type": "long"},
            "datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "is_fraud": {"type": "keyword"},
            "transaction-id": {"type": "long"},
            "type": {"type": "keyword"},
        }
    }
}

# Check if the index exists, and create it if it does not
if not es.indices.exists(index=es_index):
    try:
        # Create the Elasticsearch index if it doesn't exist
        es.indices.create(index=es_index, body=mapping)
    except RequestError as e:
        print(f"Index creation failed: {e}")
        exit(1)
    print(f"Created Elasticsearch index '{es_index}'")

# Write the streaming DataFrame to Elasticsearch
def write_to_es(es_df):
    es_df.show()
    es_df.write.format("org.elasticsearch.spark.sql").option(
        "es.nodes", es_host
    ).option("es.port", es_port).option("es.net.http.auth.user", username).option(
        "es.net.http.auth.pass", password
    ).option(
        "es.resource", es_index
    ).mode(
        "append"
    ).save()

# return if date at night
def is_night(datetime_str):
    datetime_format = "%Y-%m-%d %H:%M:%S"
    datetime_obj = datetime.datetime.strptime(datetime_str, datetime_format)
    hour = datetime_obj.hour
    
    if hour >= 22 or hour < 6:
        return 1
    else:
        return 0

# return if date in weekend
def is_weekend(datetime_str):
    datetime_format = "%Y-%m-%d %H:%M:%S"
    datetime_obj = datetime.datetime.strptime(datetime_str, datetime_format)
    weekday = datetime_obj.weekday()
    
    return 1 if weekday >= 5 else 0 # 5 and 6 correspond to Saturday and Sunday

def add_transaction_to_history(key, data):
        # Serialize the dictionary into a JSON string
        data_json = json.dumps(data)

        # Save the serialized data in Redis
        redis_client_tr.set(key, data_json)
        
        # Set the expiration time for the key
        redis_client_tr.expire(key, 7889229) # 3 months

def get_transactions_history():
    # Get all keys in Redis
    all_keys = redis_client_tr.keys("*")

    # Retrieve values for each key
    data = {}
    for key in all_keys:
        value = redis_client_tr.get(key)
        data[key.decode("utf-8")] = json.loads(value.decode("utf-8"))

    # Convert list of dictionaries to RDD
    rdd = spark.sparkContext.parallelize(list(data.values()))

    # Create DataFrame from RDD
    return spark.createDataFrame(rdd)

def add_account_to_cache(key, data):
    # Serialize the dictionary into a JSON string
    data_json = json.dumps(data)

    # Save the serialized data in Redis
    redis_client_acc.set(key, data_json)
        
    # Set the expiration time for the key
    redis_client_acc.expire(key, 604800) # 1 week

def get_account_from_cache(key):
    
    data = {}
    # Retrieve values for each key
    value = redis_client_acc.get(key)
    data[key] = json.loads(value.decode("utf-8"))

    # Convert list of dictionaries to RDD
    rdd = spark.sparkContext.parallelize(list(data.values()))

    # Create DataFrame from RDD
    return spark.createDataFrame(rdd)

def getDecimalFromKafka(encoded):

    # Decode the Base64 encoded string and create a BigInteger from it
    decoded = decimal.Decimal(
        int.from_bytes(base64.b64decode(encoded), byteorder="big", signed=False)
    )

    # Create a context object with the specified scale
    context = decimal.Context(prec=28, rounding=decimal.ROUND_HALF_DOWN)

    # Set the scale of the decimal value using the context object
    decimal_value = decoded.quantize(decimal.Decimal(".1") ** 3, context=context)

    return decimal_value / 1000000

def get_ml_transaction_input_byIds(account_id, customer_id):
    dict_ml = {}
    
    # get transaction from redis
    trans_df = get_transactions_history()

    # Filter transactions for the given customer_id
    customer_filtered_df = trans_df.filter(col("customer_id") == customer_id)

    # Calculate customer_id average amount for different time periods
    dict_ml['customer_id_avrge_amount_1day'] = customer_filtered_df.filter(col("datetime") >= date_sub(current_date(), 1)).select(avg("amount")).collect()[0][0]
    dict_ml['customer_id_avrge_amount_1week'] = customer_filtered_df.filter(col("datetime") >= date_sub(current_date(), 7)).select(avg("amount")).collect()[0][0]
    dict_ml['customer_id_avrge_amount_1month'] = customer_filtered_df.filter(col("datetime") >= date_sub(current_date(), 30)).select(avg("amount")).collect()[0][0]
    dict_ml['customer_id_avrge_amount_3month'] = customer_filtered_df.filter(col("datetime") >= date_sub(current_date(), 90)).select(avg("amount")).collect()[0][0]

    # Calculate customer_id transaction counts for different time periods
    dict_ml['customer_id_count_1day'] = customer_filtered_df.filter(col("datetime") >= date_sub(current_date(), 1)).count()
    dict_ml['customer_id_count_1week'] = customer_filtered_df.filter(col("datetime") >= date_sub(current_date(), 7)).count()
    dict_ml['customer_id_count_1month'] = customer_filtered_df.filter(col("datetime") >= date_sub(current_date(), 30)).count()
    dict_ml['customer_id_count_3month'] = customer_filtered_df.filter(col("datetime") >= date_sub(current_date(), 90)).count()

    # Filter transactions for the given account_id
    account_filtered_df = trans_df.filter(col("account_id") == account_id)

    # Calculate account_id average amount for different time periods
    dict_ml['account_id_avrge_amount_1day'] = account_filtered_df.filter(col("datetime") >= date_sub(current_date(), 1)).select(avg("amount")).collect()[0][0]
    dict_ml['account_id_avrge_amount_1week'] = account_filtered_df.filter(col("datetime") >= date_sub(current_date(), 7)).select(avg("amount")).collect()[0][0]
    dict_ml['account_id_avrge_amount_1month'] = account_filtered_df.filter(col("datetime") >= date_sub(current_date(), 30)).select(avg("amount")).collect()[0][0]
    dict_ml['account_id_avrge_amount_3month'] = account_filtered_df.filter(col("datetime") >= date_sub(current_date(), 90)).select(avg("amount")).collect()[0][0]

    # Calculate account_id transaction counts for different time periods
    dict_ml['account_id_count_1day'] = account_filtered_df.filter(col("datetime") >= date_sub(current_date(), 1)).count()
    dict_ml['account_id_count_1week'] = account_filtered_df.filter(col("datetime") >= date_sub(current_date(), 7)).count()
    dict_ml['account_id_count_1month'] = account_filtered_df.filter(col("datetime") >= date_sub(current_date(), 30)).count()
    dict_ml['account_id_count_3month'] = account_filtered_df.filter(col("datetime") >= date_sub(current_date(), 90)).count()
    
    return dict_ml

def send_email(sender, recipient, subject, message):

    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient

    # SMTP server settings for Outlook
    smtp_host = os.environ.get("smtp_host")
    smtp_port = os.environ.get("smtp_port")
    smtp_username = os.environ.get("smtp_username")
    smtp_password = os.environ.get("smtp_password")

    # Send the email
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender, recipient, msg.as_string())
        server.quit()
        print("Email sent successfully to admin!")
    except Exception as e:
        print("An error occurred while sending the email:", str(e))
        
def send_slack_alert(webhook_url, message, title):
    payload = {
        "icon_emoji": ":exclamation:",
        "username": "AI Assistant Alert",
        "attachments": [
          {
              "title": title,
              "text": message,
              "color": "danger"
          }
        ]
    }

    response = requests.post(webhook_url, json=payload)

    if response.status_code == 200:
        print("Message sent successfully to Slack.")
    else:
        print(f"Request to Slack returned an error: {response.status_code}, {response.text}")

def send_alert(dic_trans):

    # Email addresses
    sender = os.environ.get("sender")
    recipient = os.environ.get("recipient")

    subject = 'Urgent: Potential Fraud Transaction Detected - Action Required'
    message = "We have detected a suspicious transaction on a user\'s account on {datetime}.\nYour immediate attention to this matter is required.\nPlease review the details provided below:\n\nTransaction ID: {id}\nCustomer ID: {customer_id}\nAccount ID: {account_id}\nAmount: ${amount}\nDate: {datetime}.\n\nWe kindly request that you take immediate action to thoroughly investigate and address this issue to prevent any further fraudulent activity.\nYour prompt response is greatly appreciated.".format(
        datetime=dic_trans['datetime'],
        id=dic_trans['id'],
        customer_id=dic_trans['customer_id'],
        account_id=dic_trans['account_id'],
        amount=dic_trans['amount']
    )

    # slack alert
    slack_webhook_url = "https://hooks.slack.com/services/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"
    # Send the danger alert to Slack
    send_slack_alert(slack_webhook_url, message, subject)
    
    message = 'Dear Admin,\n\n' + message + '\n\nBest regards,\nApach Finracte'
    send_email(sender, recipient, subject, message)

def predict_isfraud(dic_trans):  
    # get input variable for ml
    ml_input_var = get_ml_transaction_input_byIds( dic_trans['account_id'], dic_trans['customer_id'])
    
    # create the feature vector
    dense_vector = Vectors.dense([
        dic_trans['amount'],
        ml_input_var['customer_id_avrge_amount_1day'],
        ml_input_var['customer_id_avrge_amount_1week'], 
        ml_input_var['customer_id_avrge_amount_1month'],
        ml_input_var['customer_id_avrge_amount_3month'],
        ml_input_var['customer_id_count_1day'],
        ml_input_var['customer_id_count_1week'],
        ml_input_var['customer_id_count_1month'],
        ml_input_var['customer_id_count_3month'],
        ml_input_var['account_id_avrge_amount_1day'],
        ml_input_var['account_id_avrge_amount_1week'],
        ml_input_var['account_id_avrge_amount_1month'],
        ml_input_var['account_id_avrge_amount_3month'],
        ml_input_var['account_id_count_1day'],
        ml_input_var['account_id_count_1week'],
        ml_input_var['account_id_count_1month'],
        ml_input_var['account_id_count_3month'],
        dic_trans['in_weekend'], dic_trans['at_night']
    ])

    data_vect = spark.createDataFrame([(dense_vector, ), ], ['features'])
    is_fraude = lr_model.transform(data_vect).select(['prediction']).collect()[0][0]
    
    if is_fraude == 0.0 :
        return "valid"
    else :
        # send alert to admin when fraud detected
        #send_alert(dic_trans)
        return "fraudulent"

def write_to_es_transaction(df_t, epoch_id):
    global account_df
    
    row_transactions = df_t.collect()
    
    for row_transaction in row_transactions:
    
        value_dict_transaction = json.loads(row_transaction.value)
            
        if value_dict_transaction['payload']['before'] == None :
                
            timestamp = value_dict_transaction['payload']['after']['created_date']/1000
            # convert Unix timestamp to a datetime object
            dt = datetime.datetime.fromtimestamp(timestamp)
            # format datetime object as "yyyy-mm-dd hh:mm:ss"
            dic_trans['datetime'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            formatted_date_es = dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        
            dic_trans['account_id'] = value_dict_transaction['payload']['after']['savings_account_id']
        
            while account_df.filter(col("account_id") == dic_trans['account_id']).count() == 0:
                # Wait for 0.01 second before checking again
                sleep(0.01)
                account_df = get_account_from_cache(dic_trans['account_id'])
        
            # Code to execute after the condition becomes true
            # Filter the DataFrame to get rows where "account-id" is in account_df["account-id"]
            filtered_account_df = account_df.filter(account_df["account_id"] == dic_trans['account_id'])
            # Select the "customer-id" column from the filtered DataFrame
            dic_trans['customer_id'] = filtered_account_df.select("customer_id").collect()[0][0]
                
            op_type = "DEBIT" if value_dict_transaction['payload']['after']['transaction_type_enum'] == 2 else "CREDIT"  
                
            dic_trans['amount'] = float(getDecimalFromKafka(value_dict_transaction['payload']['after']['amount']))

            dic_trans['id'] = value_dict_transaction['payload']['after']['id']
            
            is_fraud = predict_isfraud(dic_trans)
            
            # save this transacton in redis
            add_transaction_to_history(dic_trans["id"], dic_trans)
                
            dic_trans['in_weekend'] = is_night(dic_trans['datetime'])
            dic_trans['at_night'] = is_weekend(dic_trans['datetime'])
                
            new_row_transaction = spark.createDataFrame(
                [(
                    dic_trans["account_id"],
                    dic_trans["amount"],
                    dic_trans["customer_id"],
                    dic_trans["datetime"],
                    is_fraud,  # -----test after will be predected by ML
                    dic_trans["id"],
                    op_type,
                    formatted_date_es,
                    )],
                schema=schema_transaction,
            )
                        
            write_to_es(new_row_transaction)

def write_to_es_account(df_a, epoch_id):

    global account_df

    row_accounts = df_a.collect()

    for row_account in row_accounts:

        dict_account = {}
        value_dict_account = json.loads(row_account.value)
        dict_account['customer_id'] = value_dict_account['payload']['after']['client_id']
        dict_account['account_id'] = value_dict_account['payload']['after']['id']
        
        # add account to cache
        add_account_to_cache(dict_account['account_id'], dict_account)

print()
print("Start streaming...")
print()

# Call the write_to_es function on each micro-batch of data
value_df_account = df_account_stream.selectExpr("CAST(value AS STRING)")
query_account = value_df_account.writeStream.foreachBatch(write_to_es_account).start()

value_df_transaction = df_transaction_stream.selectExpr("CAST(value AS STRING)")
query_transaction = value_df_transaction.writeStream.foreachBatch(write_to_es_transaction).start()

# Wait for the stream to finish
query_account.awaitTermination()
query_transaction.awaitTermination()
