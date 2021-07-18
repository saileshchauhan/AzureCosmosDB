'''
@Author: Sailesh Chauhan
@Date: 18/07/2021
@Title: Python Script for create cosmos database at Azure.Then, create container to add JSON items as documents.
'''

import logging,sys,logconfig,family
sys.path.append('D:\Cosmos_DB\logconfig.py')
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from decouple import config

# Initialize the Cosmos client
endpoint = config('endpoint')
key = config('key')

#create_cosmos_client
client = CosmosClient(endpoint, key)

# Create a database
database_name = 'AzureSampleFamilyDatabase'
database = client.create_database_if_not_exists(id=database_name)


# Create a container
container_name = 'FamilyContainer'
container = database.create_container_if_not_exists(
    id=container_name, 
    partition_key=PartitionKey(path="/lastName"),
    offer_throughput=400
)


# Add items to the container
family_items_to_create = [family.get_andersen_family_item(), family.get_johnson_family_item(), family.get_smith_family_item(), family.get_wakefield_family_item()]


# create_item
for family_item in family_items_to_create:
    container.create_item(body=family_item)


# Read items (key value lookups by partition key and id, aka point reads)
for family in family_items_to_create:
    item_response = container.read_item(item=family['id'], partition_key=family['lastName'])
    request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
    logging.info('Read item with id {0}. Operation consumed {1} request units'.format(item_response['id'], (request_charge)))



# Query these items using the SQL query syntax. 
query = "SELECT * FROM c WHERE c.lastName IN ('Wakefield', 'Andersen')"
items = list(container.query_items(
    query=query,
    enable_cross_partition_query=True
))
request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
logging.info('Query returned {0} items. Operation consumed {1} request units'.format(len(items), request_charge))

