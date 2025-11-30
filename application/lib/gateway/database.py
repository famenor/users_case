import pandas as pd

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable

spark = SparkSession.getActiveSession()


## THIS COMPONENT CONTAINS THE CODE MODULES FOR READS AND WRITES IN DATABASES

#INTERFACE WITH METHODS THAT MUST BE DEFINED BY THE CONCRETE IMPLEMENTATIONS
class InterfaceDatabaseGateway(ABC):

    #COUNT THE TOTAL OF ROWS IN THE SPECIFIED TABLE
    @abstractmethod
    def count(self, catalog_name: str, schema_name: str, table_name: str) -> int:
        pass

    #EXECUTE A SQL QUERY IN THE DATABASE
    @abstractmethod  
    def execute_query(self, sql: str) -> list:
        pass

    #GET THE MAX INTEGER VALUE IN A COLUMN OF THE TABLE
    @abstractmethod
    def get_max_value(self, catalog_name: str, schema_name: str, table_name: str, column: str) -> int:
        pass

    #GET THE SURROGATE ID ASSOCIATED WITH A SET OF BASE VALUES
    @abstractmethod
    def get_surrogate_id(self, catalog_name: str, schema_name, table_name: str, 
                         base_values: dict, surrogate_column: str) -> int:
        pass

    #GET THE TABLE METADATA ASSOCIATED TO THE SPECIFIED TABLE
    @abstractmethod
    def get_table_metadata(self, catalog_name: str, schema_name: str, table_name: str) -> list:
        pass

    #GET THE TABLE DETAIL METADATA ASSOCIATED TO THE TABLE
    @abstractmethod
    def get_table_detail_metadata(self, table_id: int) -> list:
        pass

    #MERGE A DATAFRAME INTO AN EXISTENT TABLE
    @abstractmethod
    def merge_dataframe(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                        table_name: str, match_columns: list):
        pass

    #READ A TABLE AS DATAFRAME
    @abstractmethod
    def read_table(self, catalog_name: str, schema_name: str, table_name: str, params: dict) -> DataFrame: 
        pass

    #WRITE A DATAFRAME INTO A TABLE
    @abstractmethod
    def write_table(self, dataframe: DataFrame, catalog_name: str, schema_name: str, table_name: str, params: dict): 
        pass


#INTERFACE WITH METHODS FOR VALIDATIONS THAT MUST BE DEFINED BY THE CONCRETE IMPLEMENTATIONS
class InterfaceDatabaseScreenValidationsGateway(ABC):

    #VALIDATION SCREEN FOR IS NOT NULL
    @abstractmethod
    def get_is_not_null_failures(self, catalog_name: str, schema_name: str, table_name: str, column_name: str):
        pass

    @abstractmethod
    def get_is_unique_failures(self, catalog_name: str, schema_name: str, table_name: str, column_names: list):
        pass

    @abstractmethod
    def get_is_in_bounds_failures(self, catalog_name: str, schema_name: str, table_name: str, column_name: str, 
                                  min_value: object, max_value: object, data_type: str):
        pass

    @abstractmethod
    def get_is_in_list_failures(self, catalog_name: str, schema_name: str, table_name: str, column_name: str, list_values: list):
        pass

    @abstractmethod
    def get_is_date_format_failures(self, catalog_name: str, schema_name: str, table_name: str, column_name: str, date_format: str):
        pass

    @abstractmethod
    def get_is_not_lower_than_failures(self, catalog_name: str, schema_name: str, table_name: str, column_name: str, reference_column_name: str):
        pass


#ABSTRACT CLASS WITH THE COMMON SQL METHODS IMPLEMENTED
class AbstractSQLDatabaseGateway(InterfaceDatabaseGateway):

    ## INTERNAL METHODS

    #PARSE THE BASE VALUES FOR THE SURROGATE KEY ASSOCIATION
    def parse_base_values(self, base_values) -> dict:

        parsed_base_values = {}
        for key, value in base_values.items():
            
            if pd.isnull(value):
                raise Exception('Base value cannot be null')

            str_value = str(value).strip()
            if str_value == '':
                raise Exception('Base value cannot be empty')

            parsed_base_values[key] = str_value
        
        return parsed_base_values

    #GENERATE THE SQL STRING FOR THE SURROGATE KEY ASSOCIATION    
    def generate_conditions_string(self, parsed_base_values):

        conditions_string = []

        for key, value in parsed_base_values.items():
            conditions_string.append(f"CAST({key} AS STRING)='{value}'")
        
        conditions_string = ' AND '.join(conditions_string)
        return conditions_string
    
    #GENERATE THE SQL STRING FOR FETCHING THE SURROGATE KEY
    def generate_surrogate_fetch_query(self, catalog_name, schema_name, table_name, conditions_string, surrogate_column):

        sql = f"""SELECT {surrogate_column} AS identifier 
                  FROM {catalog_name}.{schema_name}.{table_name} 
                  WHERE {conditions_string}"""

        return sql
    

    ## IMPLEMENTATIONS AND DELEGATIONS OF THE INTERFACE METHODS

    def count(self, catalog_name: str, schema_name: str, table_name: str) -> int:
        sql = f'SELECT COUNT(*) AS count FROM {catalog_name}.{schema_name}.{table_name}'
        result = self.execute_query(sql)
        return result[0]['count']

    @abstractmethod
    def execute_query(self, sql: str) -> list:
        pass

    def get_max_value(self, catalog_name: str, schema_name: str, table_name: str, column: str): 

        sql = f"""SELECT MAX({column}) AS max_value  
                  FROM {catalog_name}.{schema_name}.{table_name}""" 

        result = self.execute_query(sql)
        return result[0]['max_value']
    
    def get_surrogate_id(self, catalog_name: str, schema_name: str, table_name: str, 
                         base_values: dict, surrogate_column: str) -> int:
        
        parsed_base_values = self.parse_base_values(base_values)
        conditions_string = self.generate_conditions_string(parsed_base_values)
        
        sql = self.generate_surrogate_fetch_query(catalog_name, schema_name, table_name, 
                                                  conditions_string, surrogate_column)
        
        result = self.execute_query(sql)

        if len(result) > 1:
            raise Exception('Unicity violation on surrogate key search')

        elif len(result) == 1:
            return result[0]['identifier']

        else:
            return None
    
    def get_table_metadata(self, catalog_name: str, schema_name: str, table_name: str):
        
        sql = f"""SELECT t.*, s.schema_name
                  FROM governance_prod.metadata.tables t
                  INNER JOIN governance_prod.metadata.schemas s ON t.schema_id = s.schema_id
                  INNER JOIN governance_prod.metadata.catalogs c ON s.catalog_id = c.catalog_id
                  WHERE table_name = '{table_name}' AND current_flag = True
                      AND s.schema_name = '{schema_name}'
                      AND c.catalog_name = '{catalog_name}' LIMIT 1"""

        result = self.execute_query(sql)
        return result
    
    def get_table_detail_metadata(self, table_id: int):

        sql = f'SELECT * FROM governance_prod.metadata.tables_detail WHERE table_id = {table_id}'

        result = self.execute_query(sql)
        return result
    
    @abstractmethod
    def merge_dataframe(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                        table_name: str, match_columns: []):
        pass

    @abstractmethod
    def read_table(self, catalog_name: str, schema_name: str, table_name: str, params: dict) -> DataFrame: 
        pass
    
    @abstractmethod
    def write_table(self, dataframe: DataFrame, catalog_name: str, schema_name: str, table_name: str, params: dict): 
        pass

#CONCRETE IMPLEMENTATION FOR THE DELTA TABLES IN DATABRICKS DATABASE    
class SparkSQLDatabaseGateway(AbstractSQLDatabaseGateway):

    def __init__(self):
        pass

    def execute_query(self, sql: str):
        return spark.sql(sql).collect()
    
    def merge_dataframe(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                        table_name: str, match_columns: []):
        
        match_string = []
        for column in match_columns:
            match_string.append(f'target.{column}=source.{column}')
        
        match_string = ' AND '.join(match_string)     
        delta_table = DeltaTable.forName(spark, f'{catalog_name}.{schema_name}.{table_name}')
    
        delta_table.alias('target').merge(dataframe.alias('source'), match_string
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def merge_dataframe_with_altered_at(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                        table_name: str, match_columns: [], altered_at: str):
        
        complete_name = f'{catalog_name}.{schema_name}.{table_name}'
        DeltaTable.createIfNotExists(spark).tableName(complete_name).addColumns(dataframe.schema).execute()

        all_case = {}
        for column in dataframe.columns:
            if not column in ['metadata_created_at', 'metadata_updated_at', 'metadata_deleted_at']:
                all_case[f'{column}'] = f'source.{column}'

        create_case = all_case.copy()
        create_case['metadata_created_at'] = f"'{altered_at}'"

        update_case = all_case.copy()
        update_case['metadata_updated_at'] = f"'{altered_at}'"

        match_string = []
        for column in match_columns:
            match_string.append(f'target.{column}=source.{column}')

        match_string = ' AND '.join(match_string)     
        delta_table = DeltaTable.forName(spark, complete_name)

        result = delta_table.alias('target').merge(dataframe.alias('source'), match_string
        ).whenMatchedUpdate(set=update_case).whenNotMatchedInsert(values=create_case).execute()

    def read_table(self, catalog_name: str, schema_name: str, table_name: str, params: dict) -> DataFrame:

        read_mode = params['read_mode']
        dataframe = None

        if read_mode == 'read_all':
            dataframe = spark.read.table(f'{catalog_name}.{schema_name}.{table_name}')

        elif read_mode == 'read_partition':
            batch_id = params['batch_id']
            partition_column = 'metadata_batch_id'
            dataframe = spark.read.table(f'{catalog_name}.{schema_name}.{table_name}').filter(f"{partition_column} = '{batch_id}'")

        elif read_mode == 'read_audit_passed':
            dataframe = spark.read.table(f'{catalog_name}.{schema_name}.{table_name}').filter(f"metadata_audit_passed = true")
        
        else:
            raise Exception('Unsupported read mode: ' + str(read_mode))

        return dataframe

    def write_table(self, dataframe: DataFrame, catalog_name: str, schema_name: str, table_name: str, params: dict): 

        write_mode = params['write_mode']
        
        if write_mode == 'overwrite_partition':

            #TO DO - ADD SUPPORT FOR NON DEFAULT PARTITION
            batch_id = params['batch_id']
            partition_column = 'metadata_batch_id'

            dataframe.write.format('delta').mode('overwrite').option('replaceWhere', f"{partition_column} = '{batch_id}'").partitionBy(partition_column).saveAsTable(f'{catalog_name}.{schema_name}.{table_name}')

        else:
            raise Exception('Unsupported write mode: ' + str(write_mode))
          