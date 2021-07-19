'''
@Author: Sailesh Chauhan
@Date: 18/07/2021
@Title: Python Script for create cosmos database at Azure.Then, create container to add JSON items as documents.
'''

import logging
import sys
sys.path.append('D:\Cosmos_DB\logconfig.py')
import logconfig
from azure.cosmos import container, exceptions, CosmosClient, PartitionKey, partition_key
import student
from decouple import config


class School:
    '''
    Description:
        School class for creating comos db connection and container then executing following methods.
    Functions:
        constructor()
        create_container()
        create_record()
        read_record()
        query_charges()
        delete_record()
        drop_database()
    '''
    def __init__(self):
        self.conn=CosmosClient(
            url=config('endpoint'),
            credential=config('key')
        )

    def create_container(self,containerName):
        '''
        Description:
            Method creates databse and container in CosmosDB account.
        Paraeters:
            containerName.
        Return:
            container.
        '''
        database = self.conn.create_database_if_not_exists(id=config('database_name'))
        container = database.create_container_if_not_exists(
        id=containerName, 
        partition_key=PartitionKey(path="/class"),
        offer_throughput=400
        )
        return container


    def create_record(self,container):
        '''
        Description:
            Method gets student details as JSON object and add them as item in container.
        Parameters:
            self,container
        Returns:
            None.
        '''
        try:
            student_items_to_create=[student.get_sam_data(),student.get_sandra_data()]
            for student_item in student_items_to_create:
                container.create_item(body=student_item)
        except Exception as ex:
            logging.exception(ex)

    def read_record(self,container):
        '''
        Description:
            Method reads all available JSON items in the container.Then, logging.info them with request units consumed.
        Parameters:
            self,container.
        Returns:
            None.
        '''
        try:
            for studnt in container.read_all_items():
                logging.info(studnt['id'],studnt['name'],studnt['class'])
                item_response = container.read_item(item=studnt['id'], partition_key=studnt['class'])
                request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
                logging.info('Read item with id {0}. Operation consumed {1} request units'.format(item_response['id'], (request_charge)))
        except Exception as ex:
            logging.exception(ex)

    def query_charges(self,container):
        '''
        Description:
            Method executes SQL query gets the result and gets request units consumed for that query.
        Parameters:
            self,container.
        Returns:
            None.
        '''
        try:
            query = "SELECT * FROM c WHERE c.name='sara'"
            items = list(container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            for item in items:
                logging.info(item['id'],item['name'])
            request_charge = container.client_connection.last_response_headers['x-ms-request-charge']
            logging.info('Query returned {0} items. Operation consumed {1} request units'.format(len(items), request_charge))
        except Exception as ex:
            logging.exception(str(ex))

    def delete_record(self,container):
        '''
        Description:
            Delete a record in the container
        Parameter:
            self,container
        Return:
            None
        '''
        try:
            Dquery = "SELECT * FROM c WHERE c.name = 'sandra'"
            studentList=list(container.query_items(query = Dquery, enable_cross_partition_query = True))
            for items in studentList:
                container.delete_item(items['id'],partition_key='third')
            logging.info("Delete Record Successfull")
        except Exception:
            logging.exception("Delete Record Unsuccessfull")

    def drop_database(self,databaseName):
        '''
        Description:
            Delete the created Database
        Parameter:
            self,databaseName
        Return:
            None
        '''
        try:
            self.conn.delete_database(databaseName)
            logging.info("Database {} dropped successfully ".format(databaseName))
        except Exception:
            logging.exception("Drop Database Unsuccessfull")

def main():
    '''
    Description:
        Driver method for executing all methods in script.
    '''
    std=School()
    containerName='studentRecord'
    container=std.create_container(containerName)
    std.create_record(container)
    std.read_record(container)
    std.query_charges(container)
    std.delete_record(container)
    std.drop_database(databaseName=config('database_name'))

if __name__=='__main__':
    main()