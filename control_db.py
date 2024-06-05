from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import time

load_dotenv()

db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_access_secret_key = os.getenv('AWS_ACCESS_SECRET_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION')

db_instance_identifier = 'mydb'

rds = boto3.client('rds',
                   aws_access_key_id=aws_access_key,
                   aws_secret_access_key=aws_access_secret_key,
                   region_name=aws_region)

def start_db_instance(db_instance_identifier):
    try:
        response = rds.start_db_instance(DBInstanceIdentifier=db_instance_identifier)
        return response
    except Exception as e:
        print(f"Error starting DB instance: {e}")

def stop_db_instance(db_instance_identifier):
    try:
        response = rds.stop_db_instance(DBInstanceIdentifier=db_instance_identifier)
        return response
    except Exception as e:
        print(f"Error stopping DB instance: {e}")

def get_db_instance_status(db_instance_identifier):
    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
        status = response['DBInstances'][0]['DBInstanceStatus']
        return status
    except Exception as e:
        print(f"Error retrieving DB instance status: {e}")


def check_status():
    status = get_db_instance_status(db_instance_identifier)
    print("Current Status:", status)
    return status


def start_instance():
    status = get_db_instance_status(db_instance_identifier)
    if status == 'stopped':
        print("Starting the DB instance...")
        start_response = start_db_instance(db_instance_identifier)
        #print("Start Response:", start_response)
        status = get_db_instance_status(db_instance_identifier)
        while status == 'starting':
            # print('Checking instance Status: ', status)
            time.sleep(1)
            status = get_db_instance_status(db_instance_identifier)
        if status == 'available':
            print('Instance started successfully')
    elif status == 'available':
        print('Instance is already available')
    else:
        print('Do not do anything, instance status now: ', status)
    

def stop_instance():
    status = get_db_instance_status(db_instance_identifier)
    if status == 'available':
        print("Stopping the DB instance...")
        stop_response = stop_db_instance(db_instance_identifier)
        #print("Stop Response:", stop_response)
        status = get_db_instance_status(db_instance_identifier)
        while status == 'stopping':
            # print('Instance Status: ', status)
            time.sleep(1)
            status = get_db_instance_status(db_instance_identifier)
        if status == 'stopped':
            print('Instance stopped successfully')
    elif status == 'stopped':
        print('Instance is already stopped')
    else:
        print('Do not do anything, instance status now: ', status)

if __name__ == '__main__':
    pass
    # print('test')
