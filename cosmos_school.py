'''
@Author: Sailesh Chauhan
@Date: 18/07/2021
@Title: Python Script for create cosmos database at Azure.Then, create container to add JSON items as documents.
'''

import logging
import sys
sys.path.append('D:\Cosmos_DB\logconfig.py')
import logconfig
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import student
from decouple import config

endpoint=config('endpoint')
key=config('key')

myclient = CosmosClient(endpoint, key)

database_name = config('database_name')
database = myclient.create_database_if_not_exists(id=database_name)

container_name = config('container_name')

container = database.create_container_if_not_exists(
    id=container_name, 
    partition_key=PartitionKey(path="/class"),
    offer_throughput=400
)

def create_record():
    '''
    Description:
        Method gets student details as JSON object and add them as item in container.
    Parameters:
        None.
    Returns:
        None.
    '''
    student_items_to_create=[student.get_sam_data(),student.get_sandra_data()]
    for student_item in student_items_to_create:
        container.create_item(body=student_item)


def read_record():
    '''
    Description:
        Method reads all available JSON items in the container.Then, logging.info them with request units consumed.
    Parameters:
        None.
    Returns:
        None.
    '''
    for studnt in container.read_all_items():
        logging.info(studnt['id'],studnt['name'],studnt['class'])
        item_response = container.read_item(item=studnt['id'], partition_key=studnt['class'])
        request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
        logging.info('Read item with id {0}. Operation consumed {1} request units'.format(item_response['id'], (request_charge)))

def query_charges():
    '''
    Description:
        Method executes SQL query gets the result and gets request units consumed for that query.
    Parameters:
        None.
    Returns:
        None.
    '''
    query = "SELECT * FROM c WHERE c.name='sara'"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    for item in items:
        logging.info(item['id'],item['name'])
    request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
    logging.info('Query returned {0} items. Operation consumed {1} request units'.format(len(items), request_charge))

create_record()
read_record()
query_charges()