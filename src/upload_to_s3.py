import boto3
import configparser
import os
import logging
from botocore.exceptions import NoCredentialsError
#from s3_module import GoodReadsS3Module
from pyspark.sql import SparkSession


# Set JAVA_HOME
os.environ['JAVA_HOME'] = 'C:\\java2'
os.environ['HADOOP_HOME'] = 'C:\hadoop'
os.environ['SPARK_HOME'] = 'C:\spark-3.5.1-bin-hadoop3'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ['HADOOP_HOME'] + '\\lib\\native;' + os.environ['JAVA_HOME'] + '\\bin;' + os.environ['SPARK_HOME'] + '\\bin;' + os.environ['PATH']
# # Update PATH
# os.environ['PATH'] = os.environ['JAVA_HOME'] + '\\bin;' + os.environ['PATH']


# Set environment variables for PySpark
#os.environ['PYSPARK_PYTHON'] = r"C:/Users/tuank/anaconda3/python.exe"
#os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:/Users/tuank/anaconda3/python.exe'

# Configure logging
logging.basicConfig(
    filename='upload_to_s3.log',  # Log file name
    level=logging.INFO,           # Logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'  # Log format
)

# config = configparser.ConfigParser()
# config.read('dwh.cfg')
# os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_ID'] = config['AWS_SECRET_ACCESS_ID']

def read_config(file_path='dwh.cfg'):
    config = configparser.ConfigParser()
    try:
        config.read(file_path)
        if 'dwh' not in config:
            raise configparser.NoSectionError('dwh')
        aws_access_key_id = config.get('dwh', 'AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get('dwh', 'AWS_SECRET_ACCESS_ID')
        region = config.get('dwh', 'region')
        
        logging.info(f"Config read successfully: region={region}, aws_access_key_id={aws_access_key_id}")
        print(f"Config read successfully: region={region}, aws_access_key_id={aws_access_key_id}")
        
        return aws_access_key_id, aws_secret_access_key, region
    except configparser.NoSectionError as e:
        logging.error(f"Section 'dwh' not found in the config file: {e}")
        print(f"Section 'dwh' not found in the config file: {e}")
    except configparser.NoOptionError as e:
        logging.error(f"Option not found in the 'dwh' section: {e}")
        print(f"Option not found in the 'dwh' section: {e}")
    except Exception as e:
        logging.error(f"Unexpected error reading the config file: {e}")
        print(f"Unexpected error reading the config file: {e}")


def upload_to_s3(local_directory, bucket):

    # config = configparser.ConfigParser()
    # config.read('dwh.cfg')
    # if 'dwh' not in config:
    #     raise configparser.NoSectionError('dwh')
    # aws_access_key_id = config.get('dwh', 'AWS_ACCESS_KEY_ID')
    # aws_secret_access_key = config.get('dwh', 'AWS_SECRET_ACCESS_ID')
    # region = config.get('dwh', 'region')


    aws_access_key_id, aws_secret_access_key, region = read_config()
    if not all([aws_access_key_id, aws_secret_access_key, region]):
        logging.error("Missing AWS configuration. Exiting.")
        print("Missing AWS configuration. Exiting.")
        return
    
    session = boto3.Session(region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3 = session.client(
        's3')
    
    # try:
    #     for file_name in os.listdir(local_directory):
    #         if file_name.endswith('.csv'):
    #             local_file = os.path.join(local_directory, file_name)
    #             s3_file = file_name
    #             s3.upload_file(local_file, bucket, s3_file)
    #             logging.info(f"Upload Successful: {local_file} to {bucket}/{s3_file}")
    # except FileNotFoundError:
    #     logging.error(f"The file {local_file} was not found")
    # except NoCredentialsError:
    #     logging.error("Credentials not available")
    try:
        for root, dirs, files in os.walk(local_directory):
            for file_name in files:
                if file_name.endswith('.parquet'):
                    local_file = os.path.join(root, file_name)
                    s3_file = os.path.relpath(local_file, local_directory)
                    s3.upload_file(local_file, bucket, s3_file)
                    s3.upload_file(local_file, bucket, s3_file)
                    os.remove(local_file)
                    logging.info(f"Upload Successful: {local_file} to {bucket}/{s3_file}")
    except FileNotFoundError as e:
        logging.error(f"The file {e.filename} was not found")
    except NoCredentialsError:
        logging.error("Credentials not available")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

def convert_csv_to_parquet(csv_directory, parquet_directory):
    if not os.path.exists(parquet_directory):
        os.makedirs(parquet_directory)
    custom_temp_dir = ".\\temp"
    os.makedirs(custom_temp_dir, exist_ok=True)
    spark = SparkSession.builder \
        .appName("CSV to Parquet Conversion") \
        .config("spark.local.dir",custom_temp_dir) \
        .getOrCreate()

    for file_name in os.listdir(csv_directory):
        if file_name.endswith('.csv'):
            csv_file_path = os.path.join(csv_directory, file_name)
            df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
            
            if 'author_id' in df.columns:
                partition_col = 'author_id'
            elif 'user_id' in df.columns:
                partition_col = 'user_id'
            else:
                logging.warning(f"No partition column found in {csv_file_path}")
                continue

            parquet_file_path = os.path.join(parquet_directory, file_name.replace('.csv', ''))
            df.write.partitionBy(partition_col).parquet(parquet_file_path, mode='overwrite')
            logging.info(f"Converted {csv_file_path} to {parquet_file_path}")

    spark.stop()

def main():
    csv_directory = "../SampleData"
    parquet_directory = "../ParquetData"
    bucket_name = "pawspaws-working"

    # Convert CSV to Parquet with partitioning
    convert_csv_to_parquet(csv_directory, parquet_directory)

    # Upload Parquet files to S3
    upload_to_s3(parquet_directory, bucket_name)
    # print(read_config())

if __name__ == "__main__":

    main()


# def main():
#     upload_to_s3("..\SampleData", 'pawspaws')


# import configparser
# import logging
# import os

# # Configure logging
# logging.basicConfig(
#     filename='upload_to_s3.log',  # Log file name
#     level=logging.INFO,           # Logging level
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'  # Log format
# )

# def read_config(file_path='dwh.cfg'):
#     config = configparser.ConfigParser()
#     try:
#         config.read(file_path)
#         if 'dwh' not in config:
#             raise configparser.NoSectionError('dwh')
#         aws_access_key_id = config.get('dwh', 'AWS_ACCESS_KEY_ID')
#         aws_secret_access_key = config.get('dwh', 'AWS_SECRET_ACCESS_ID')
#         region = config.get('dwh', 'region')
        
#         logging.info(f"Config read successfully: region={region}, aws_access_key_id={aws_access_key_id}")
#         print(f"Config read successfully: region={region}, aws_access_key_id={aws_access_key_id}")
        
#         return aws_access_key_id, aws_secret_access_key, region
#     except configparser.NoSectionError as e:
#         logging.error(f"Section 'dwh' not found in the config file: {e}")
#         print(f"Section 'dwh' not found in the config file: {e}")
#     except configparser.NoOptionError as e:
#         logging.error(f"Option not found in the 'dwh' section: {e}")
#         print(f"Option not found in the 'dwh' section: {e}")
#     except Exception as e:
#         logging.error(f"Unexpected error reading the config file: {e}")
#         print(f"Unexpected error reading the config file: {e}")

# def main():
#     aws_access_key_id, aws_secret_access_key, region = read_config()
#     if aws_access_key_id and aws_secret_access_key and region:
#         # Proceed with your logic here
#         pass
#     else:
#         logging.error("Failed to read configuration. Exiting.")
#         print("Failed to read configuration. Exiting.")

# if __name__ == "__main__":
#     main()
