'''
@Author: Sailesh Chauhan
@Date: 21/07/2021
@Title: Python Script for creating cosmos database using SQL API.Then, saving a stored procedure in database
        container. Using stored procedure to insert items in container.
'''

import logging,sys
sys.path.append('D:\Cosmos_DB\logconfig.py')
import azure.cosmos.cosmos_client as cosmos_client
from azure.cosmos import PartitionKey, partition_key
from decouple import config


class StoredProcedureGrocery:
    '''
    Description:
        StoredProcedureGrocery class connects with SQL API of cosmos db and creates database and 
        container. Then, a stored procedure can be saved and used for creating new items in the container.
    Functions:
        __init__()
        create_sp()
        insert_item_using_sp()
    '''

    def __init__(self):
        url=config('endpoint')
        key=config('key')
        self.client = cosmos_client.CosmosClient(url,key)
        self.database = self.client.create_database_if_not_exists(id=config('sp_database_name'))
        self.container = self.database.create_container_if_not_exists(
                id=config('sp_container_name'),
                partition_key=PartitionKey(path='/id'),
                offer_throughput=400
                )

    def create_sp(self):
        '''
        Description:
            Method creates a stored procedure in a container inside SQL API cosmos database.
        Parameters:
            Object of same class.
        Returns:
            None.
        '''
        try:
            with open('D:\Cosmos_DB\SP_SQL_API\spCreateToDoItems.js') as file:
                file_contents = file.read()
            sproc = {
                'id': 'spCreateToDoItem',
                'serverScript': file_contents,
            }
            created_sproc = self.container.scripts.create_stored_procedure(body=sproc)
            logging.info('sp created {} '.format(sproc['id']))
        except Exception as ex:
            logging.info(ex)

    def insert_item_using_sp(self,new_item):
        '''
        Description:
            Method inserts item using stored prodcedure saved in container.
        Parameters:
            Object of same class.
        Returns:
            None.
        '''
        try:
            result = self.container.scripts.execute_stored_procedure(sproc='spCreateToDoItem',params=[[new_item]], partition_key=new_item['id'])
            logging.info(result)
        except Exception as ex:
            logging.info(ex)

def main():
    '''
    Description: Main driver method for calling all methods.
    '''
    # Creating a document for a container with "id" as a partition key. 
    import uuid
    new_id= str(uuid.uuid4())
    new_item =   {
        "id": new_id, 
        "category":"Personal",
        "name":"Groceries",
        "description":"Pick up strawberries",
        "isComplete":False
    }
    job=StoredProcedureGrocery()
    job.create_sp()
    job.insert_item_using_sp(new_item)

if __name__=="__main__":
     main()