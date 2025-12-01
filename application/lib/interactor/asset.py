import pandas as pd

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from lib.gateway.database import InterfaceDatabaseGateway


## THIS COMPONENT CONTAINS THE CODE MODULES FOR INTERACTORS WITH ASSETS

#INTERFACE WITH METHODS THAT MUST BE DEFINED BY THE CONCRETE IMPLEMENTATIONS 
class InterfaceAssetInteractor(ABC):

    @abstractmethod
    def merge_dataframe(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                        table_name: str, match_columns: list):
        pass

    @abstractmethod
    def get_total_rows(self, catalog_name: str, schema_name: str, table_name: str):
        pass

    @abstractmethod
    def has_batch_been_processed(self, catalog_name: str, schema_name: str, table_name: str, batch_id: str) -> bool:
        pass

    @abstractmethod
    def read_table(self, catalog_name: str, schema_name: str, table_name: str, params: dict) -> DataFrame:
        pass

    @abstractmethod
    def write_table(self, dataframe: DataFrame, catalog_name: str, schema_name: str, table_name: str, params: dict):
        pass
    

#USE CASES FOR ASSETS
class AssetInteractor(InterfaceAssetInteractor):

    def __init__(self, database_gateway: InterfaceDatabaseGateway):
        self.database_gateway = database_gateway  
    
    #MERGE A DATAFRAME INTO A TABLE IN THE DATABASE
    def merge_dataframe(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                        table_name: str, match_columns: list):  
             
        self.database_gateway.merge_dataframe(dataframe=dataframe, 
                                             catalog_name=catalog_name, 
                                             schema_name=schema_name, 
                                             table_name=table_name, 
                                             match_columns=match_columns)
        
    #MERGE A DATAFRAME INTO A TABLE IN THE DATABASE WITH ALTERED AT DATES
    def merge_dataframe_with_altered_at(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                                        table_name: str, match_columns: [], altered_at: str):
        
        self.database_gateway.merge_dataframe_with_altered_at(dataframe=dataframe, 
                                                              catalog_name=catalog_name, 
                                                              schema_name=schema_name, 
                                                              table_name=table_name, 
                                                              match_columns=match_columns, 
                                                              altered_at=altered_at)
    
    #GET THE TOTAL OF ROWS IN A TABLE
    def get_total_rows(self, catalog_name: str, schema_name: str, table_name: str):
        
        total_rows = self.database_gateway.count(catalog_name, schema_name, table_name)
        return total_rows
    
    #CHECK IF A BATCH HAS ALREADY BEEN PROCESSED
    def has_batch_been_processed(self, catalog_name: str, schema_name: str, table_name: str, batch_id: str) -> bool:
        
        query = f"""SELECT COUNT(*) AS count 
                    FROM governance_prod.metrics.ingestions 
                    WHERE catalog_name = '{catalog_name}' AND schema_name = '{schema_name}' 
                    AND table_name = '{table_name}' AND batch_id = '{batch_id}'"""

        result = self.database_gateway.execute_query(query)
        count = result[0]['count']
            
        if count > 1:
            raise Exception('Inconsistency in the database. More than one record found for the same batch_id.')

        elif count == 1:
            return True
        
        else:
            return False
    
    #READ A TABLE IN THE DATABASE
    def read_table(self, catalog_name: str, schema_name: str, table_name: str, params: dict) -> DataFrame:
        return self.database_gateway.read_table(catalog_name, schema_name, table_name, params)
    
    #WRITE A TABLE IN THE DATABASE
    def write_table(self, dataframe: DataFrame, catalog_name: str, schema_name: str, table_name: str, params: dict):

        self.database_gateway.write_table(dataframe=dataframe, 
                                          catalog_name=catalog_name, 
                                          schema_name=schema_name, 
                                          table_name=table_name, 
                                          params=params)