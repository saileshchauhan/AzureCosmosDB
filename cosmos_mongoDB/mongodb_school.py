'''
@Author: Sailesh Chauhan
@Date: 19/07/2021
@Title: Python Script for create cosmos mongo database at Azure.Then, create collection to add JSON items as documents.
'''

import sys,pymongo,logging,student
sys.path.append('D:\Cosmos_DB')
import logconfig
from decouple import config

class School:
    '''
    Description: 
        This School class contains method for connecting with cosmos DB using python and performing
        CRUD operations.
    Functions:
        __init__()
        create_coll()
        insert_record()
        retrieve_items()
        update_record()
        delete_record()
        delete_collection()

    '''
    def __init__(self,conn_string):
        self.conn=pymongo.MongoClient(conn_string)
        self.mydb=self.conn['school']
        self.coll=self.create_coll('records')

    def create_coll(self,collection):
        '''
        Description:
            Method creates a collection and return that collection.
        Parameters:
            collection name as parameters.
        Returns:
            coll.
        '''
        try:
            coll=self.mydb[collection]
            return coll
        except Exception as ex:
            logging.info(ex)

    def insert_record(self,docList):
        '''
        Description:
            Method inserts all items in the collection created.
        Parameters:
            self class object and document list in JSON format.
        Returns:
            None.
        '''
        try:
            self.coll.insert_many(documents=docList)
        except Exception as ex:
            logging.info(ex)

    def retrieve_items(self):
        '''
        Description:
            Method retrieve all items from collection and log them.
        Parameters:
            self class object.
        Returns:
            None.
        '''        
        try:
            for items in self.coll.find():
                logging.info(items)
        except Exception as ex:
            logging.info(ex)

    def update_record(self,update):
        '''
        Description:
            Method updates item using update_one method.
        Parameters:
            self object of School class and update values.
        Returns:
            None.
        '''
        try:
            if update in [student.sam_item['_id'],student.sandra_item['_id']]:
                self.coll.update_one({'_id':'{}'.format(update)},{'$set':{'class':'four'}})
            else:
                logging.info('Record Updation unsuccesfull')
        except Exception as ex:
            logging.info(ex)

    def delete_record(self,deleteId):
        '''
        Description:
            Method deletes record using delete_one().
        Parameters:
            object of class school and deleteId as document id.
        Returns:
            None.
        '''
        try:
            if deleteId in [student.sam_item['_id'],student.sandra_item['_id']]:
                self.coll.delete_one({'_id':'{}'.format(deleteId)})
        except Exception as ex:
            logging.info(ex)

    def delete_collection(self):
        '''
        Description:
            Method deletes collection already created using drop method.
        Parameters:
            self object of class school.
        Returns:
            None.
        '''
        try:
            self.coll.drop()
        except Exception as ex:
            logging.info(ex)

    def drop_databases(self):
        '''
        Description:
            Method drop complete database using connection string.
        Parameters:
            object of class school.
        Returns:
            None.
        '''
        try:
            self.conn.drop_database('school')
        except Exception as ex:
            print(ex)
            
def main():
    '''
    Description:
        Driver method for calling all methods
    Parameters:
        None.
    Returns:
        None.
    '''       
    std=School(conn_string=config('conn_string'))
    std.insert_record([student.sam_item,student.sandra_item])
    std.retrieve_items()
    std.update_record('1')
    std.delete_record('2')
    std.delete_collection()
    std.drop_databases()

if __name__=="__main__":
    main()
