import pandas as pd

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from lib.gateway.database import InterfaceDatabaseGateway


## THIS COMPONENT CONTAINS THE CODE MODULES FOR INTERACTORS WITH GOVERNANCE

#INTERFACE WITH METHODS THAT MUST BE DEFINED BY THE CONCRETE IMPLEMENTATIONS 
class InterfaceGovernanceInteractor(ABC):

    @abstractmethod
    def get_catalog_id(self, catalog_name: str) -> int:
        pass

    @abstractmethod
    def get_schema_id(self, catalog_id: int, schema_name: str) -> int:
        pass

    @abstractmethod
    def get_table_metadata(self, catalog_name: str, schema_name: str, table_name: str):
        pass

#USE CASES FOR GOVERNANCE
class GovernanceInteractor:

    def __init__(self, database_gateway: InterfaceDatabaseGateway):
        self.database_gateway = database_gateway  

    #GET THE IDENTIFIER OF A CATALOG
    def get_catalog_id(self, catalog_name):

        base_values = {'catalog_name': catalog_name}

        surrogate_id = self.database_gateway.get_surrogate_id(catalog_name='governance_prod', 
                                                              schema_name='metadata', 
                                                              table_name='catalogs', 
                                                              base_values=base_values,
                                                              surrogate_column='catalog_id')

        if pd.isnull(surrogate_id):
            raise Exception('Catalog not found')

        return surrogate_id

    #GET THE IDENTIFIER OF A SCHEMA
    def get_schema_id(self, catalog_id, schema_name):

        base_values = {'catalog_id': catalog_id, 'schema_name': schema_name}

        surrogate_id = self.database_gateway.get_surrogate_id(catalog_name='governance_prod', 
                                                              schema_name='metadata', 
                                                              table_name='schemas', 
                                                              base_values=base_values,
                                                              surrogate_column='schema_id')

        if pd.isnull(surrogate_id):
            raise Exception('Schema not found')

        return surrogate_id

    #GET THE METADATA OF THE TABLE AND ITS DETAILS    
    def get_table_metadata(self, catalog_name: str, schema_name: str, table_name: str):
        
        metadata = {}
        metadata_table = self.database_gateway.get_table_metadata(catalog_name, schema_name, table_name) 
        
        #SET CATALOG, SCHEMA AND ETL MODULE
        table_id = metadata_table[0]['table_id']
        metadata['table_id'] = table_id
        metadata['catalog_name'] = catalog_name
        metadata['schema_name'] = schema_name
        metadata['table_name'] = table_name
        metadata['etl_module'] = metadata_table[0]['etl_module']
        metadata['quality'] = metadata_table[0]['quality']
        metadata['write_mode'] = metadata_table[0]['write_mode']
        metadata['field_data'] = {}

        #GET TABLE DETAILS  
        metadata_table_detail = self.database_gateway.get_table_detail_metadata(table_id)

        for row in metadata_table_detail:

            if 'metadata' in row['column_name']:
                continue

            metadata['field_data'][row['column_name']] = {'data_type': row['data_type'], 
                                                          'rename_from': row['rename_from'],
                                                          'is_primary_key': row['is_primary_key'],
                                                          'is_nullable': row['is_nullable'], 
                                                          'is_partition': row['is_partition'], 
                                                          'is_pii': row['is_pii'], 
                                                          'validations': row['validations'],
                                                          'track_changes': row['track_changes'],
                                                          'comment': row['comment']}
            
        return metadata