import io
import json
import logging
import psycopg2
import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError
from datetime import date, datetime
from typing import Any

logger = logging.getLogger('__name__')
logger.setLevel(logging.INFO)

def prepare_sql_statement_success(
    unique_id,
    db_table,
    status,
    response_object
):
    return f"""
        UPDATE {db_table} SET status='{status}', result_data='{response_object} WHERE unique_id='{unique_id}'
    """

def prepare_sql_statement_failure(
    unique_id,
    db_table,
    status,
    response_object
):
    return f"""
        UPDATE {db_table} SET status='{status}' WHERE unique_id='{unique_id}'
    """

def prepare_sql_statement_callback_failure(
    unique_id,
    db_table
):
    # note: status = 3 (Retrying)
    now_date = datetime.now().isoformat()
    return f"""
        INSERT INTO {db_table} (request_unique_id, created_at, modified_at, retries_count, status) 
        VALUES ('{unique_id}', '{now_date}', '{now_date}', 0, 3)
    """

def status_update_db(
    db_conn,
    db_cursor,
    sql_statement: str
) -> None:
    """
    Updates the status in the database
    """
    if db_cursor:
        try:
            db_cursor.execute(sql_statement)
            logger.info("Db updated. Number of rows affected: %s", db_cursor.rowcount)
            db_conn.commit()
            db_cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
        finally:
            if db_conn is not None:
                db_conn.close()


def generate_presigned_url(
    bucket_name:str,
    key:str,
    aws_region: str="us-east-1",
    signed_url_expiry_secs: int=86400
):
    """
    Generates a presigned url of the file stored in s3
    """
    # Note that the bucket and service(e.g. summarization) should run on the same aws region
    try:
        s3_client = boto3.client(
            "s3",
            region_name=aws_region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"}
            )
        )
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": bucket_name,
                "Key": key
            },
            ExpiresIn=signed_url_expiry_secs
        )
    except ClientError as cexc:
        logger.error("Error while generating presigned url %s", cexc)
        return None
    return url

def upload_to_s3(
    contents: Any,
    contents_type: str,
    bucket_name: str,
    key: str,
    aws_region: str="us-east-1"
):
    """
    Stores the summary in s3 and generate presigned url
    """
    try:
        session = boto3.Session()
        s3_resource = session.resource("s3")
        bucket = s3_resource.Bucket(bucket_name)
        contents_bytes = bytes(contents, "utf-8")
        contents_bytes_obj = io.BytesIO(contents_bytes)
        bucket.upload_fileobj(
            contents_bytes_obj,
            key,
            ExtraArgs={"ContentType": f"{contents_type}"}
        )
        return generate_presigned_url(
            bucket_name,
            key,
            aws_region=aws_region
        )
    except ClientError as cexc:
        logging.error(str(cexc))
        return None

def send_request_on_callback(
    client_id: str,
    callback_url: str,
    presigned_url: str,
    status: int,
    headers: str,
):
    """
    Sends the results in a callback url
    """
    try:
        response = requests.post(
            callback_url,
            headers=headers,
            data=json.dumps({
                "client_id": client_id,
                "presigned_s3_url": presigned_url,
                "status": status
            }),
            timeout=30
        )
    except requests.exceptions.RequestException as rexc:
        logger.error("Exception occurred while sending request %s", str(rexc))
        return None
        
    if response.status_code == 200:
        logger.info("Successfully sent the request on callback url")
        return response
    else:
        logger.error("Error while sending the request on callback url")
        return None

def update_db_table_callback_retry(
    db_conn,
    db_cursor,
    unique_id: str,
    db_table: str
):
    """
    Updates the table whenever the callback fails
    """
    if db_table and unique_id:
        sql_statement = prepare_sql_statement_callback_failure(unique_id, db_table)
        status_update_db(
            db_conn,
            db_cursor,
            sql_statement
        )
        logger.info("Updated the db table for callback retries.")
    else:
        logger.error(f"Failed to update table {db_table} for callback retries. Some missing fields")


class Database:
    """
    Class to handle database connections
    """
    def __init__(
        self,
        endpoint: str,
        database: str,
        username: str,
        password: str,
        port: int=5432,
    ):
        self.endpoint = endpoint
        self.database = database
        self.username = username
        self.password = password
        self.port = port

    def db_connection(self):
        """
        Establish database connections
        """
        try:
            conn = psycopg2.connect(
                host=self.endpoint,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            cur = conn.cursor()
            return conn, cur
        except Exception as exc:
            logger.error("Database connection failed %s", exc)
            return None, None