import os
import json
import datetime
import base64
import decimal

from time import sleep
from dotenv import load_dotenv

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
from pyspark.sql.functions import col


os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.16.2,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.13.0 pyspark-shell"

# create a Spark session
spark = SparkSession.builder.appName("kafka_elastic_test1").getOrCreate()

# create a Kafka stream for transaction
df_transaction_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "dbserver1.fineract_default.m_savings_account_transaction")
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
schema_account = StructType(
    [
        StructField("customer-id", LongType(), True),
        StructField("account-id", LongType(), True),
    ]
)

account_df = spark.createDataFrame([], schema=schema_account)


# Define the Elasticsearch index name
es_index = "transactions_index"
es_port = 9200
es_host = "elasticsearch"


# Load environment variables from .env file
load_dotenv()

# Access the username and password
username = os.environ.get("USERNAME")
password = os.environ.get("PASSWORD")

# Create an Elasticsearch client
es = Elasticsearch(
    [{"host": es_host, "port": es_port, "scheme": "http"}],
    basic_auth=(username, password),
)

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


def write_to_es_transaction(df_t, epoch_id):

    row_transactions = df_t.collect()

    for row_transaction in row_transactions:

        # if(row_transaction):
        value_dict_transaction = json.loads(row_transaction.value)

        if value_dict_transaction["payload"]["before"] == None:

            timestamp = (
                value_dict_transaction["payload"]["after"]["created_date"] / 1000
            )
            # convert Unix timestamp to a datetime object
            dt = datetime.datetime.fromtimestamp(timestamp)
            # format datetime object as "yyyy-mm-dd hh:mm:ss"
            formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
            formatted_date_es = dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

            account_id = value_dict_transaction["payload"]["after"][
                "savings_account_id"
            ]

            while account_df.filter(col("account-id") == account_id).count() == 0:
                # Wait for 0.1 second before checking again
                sleep(0.1)

            # Code to execute after the condition becomes true
            # Filter the DataFrame to get rows where "account-id" is in account_df["account-id"]
            filtered_account_df = account_df.filter(
                account_df["account-id"] == account_id
            )
            # Select the "customer-id" column from the filtered DataFrame
            cutomer_id = filtered_account_df.select("customer-id").collect()[0][0]
            op_type = (
                "DEBIT"
                if value_dict_transaction["payload"]["after"]["transaction_type_enum"]
                == 2
                else "CREDIT"
            )

            new_row_transaction = spark.createDataFrame(
                [
                    (
                        account_id,
                        float(
                            getDecimalFromKafka(
                                value_dict_transaction["payload"]["after"]["amount"]
                            )
                        ),
                        cutomer_id,
                        formatted_date,
                        "valid",  # -----test after will be predected by ML
                        value_dict_transaction["payload"]["after"]["id"],
                        op_type,
                        formatted_date_es,
                    )
                ],
                schema=schema_transaction,
            )

            write_to_es(new_row_transaction)


def write_to_es_account(df_a, epoch_id):

    global account_df

    row_accounts = df_a.collect()

    for row_account in row_accounts:

        # if(row_account):
        value_dict_account = json.loads(row_account.value)
        new_row_account = spark.createDataFrame(
            [
                (
                    value_dict_account["payload"]["after"]["client_id"],
                    value_dict_account["payload"]["after"]["id"],
                )
            ],
            schema=schema_account,
        )

        # Check if new_row_account is already present in acount_df
        if account_df.subtract(new_row_account).count() == account_df.count():
            # new_row_account does not exist in acount_df, so concatenate the two DataFrames
            account_df = account_df.union(new_row_account)


# Call the write_to_es function on each micro-batch of data
value_df_account = df_account_stream.selectExpr("CAST(value AS STRING)")
query_account = value_df_account.writeStream.foreachBatch(write_to_es_account).start()

value_df_transaction = df_transaction_stream.selectExpr("CAST(value AS STRING)")
query_transaction = value_df_transaction.writeStream.foreachBatch(
    write_to_es_transaction
).start()

# Wait for the stream to finish
query_account.awaitTermination()
query_transaction.awaitTermination()
