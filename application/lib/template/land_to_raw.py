import pandas as pd
from abc import ABC, abstractmethod

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable

from lib.gateway.file import *
from lib.gateway.database import *
from lib.interactor.governance import *
from lib.interactor.asset import *

spark = SparkSession.getActiveSession()


## FACTORIES FOR COMPONENTS REQUIRED BY THE TEMPLATE

#ABTRACT FACTORY WITH METHODS TO BE IMPLEMENTED
class AbstractFactoryLandToRaw(ABC):

    @abstractmethod
    def create(self):
        pass

#CONCRETE FACTORY FOR TEMPLATE BASED ON CSV FILE WITH COMMA SEPARATOR
class FactoryLandToRawFromCsv(AbstractFactoryLandToRaw):

    def __init__(self, path):
        self.path = path
        
    def create(self):

        database_gateway = SparkSQLDatabaseGateway()
        self.table_reader = CsvFileReader(file_path=self.path)
  
        self.governance_interactor = GovernanceInteractor(database_gateway=database_gateway)
        self.asset_interactor = AssetInteractor(database_gateway=database_gateway)

#CONCRETE FACTORY FOR TEMPLATE BASED ON CSV FILE WITH SEMICOLON SEPARATOR
class FactoryLandToRawFromCsvWithSemicolon(AbstractFactoryLandToRaw):

    def __init__(self, path):
        self.path = path
        
    def create(self):

        database_gateway = SparkSQLDatabaseGateway()
        self.table_reader = CsvFileReader(file_path=self.path)
        self.table_reader.set_separator(separator=';')
  
        self.governance_interactor = GovernanceInteractor(database_gateway=database_gateway)
        self.asset_interactor = AssetInteractor(database_gateway=database_gateway)


## TEMPLATE FOR EXTRACTING A TABLE AND METADATA, PROCESSING THE TABLE AND EXPORTING THE TABLE WITH METADATA AND METRICS

#INTERFACE FOR THE TEMPLETE
class InterfaceLandToRawTemplate(ABC):

    @abstractmethod
    def set_component_factory(self, component_factory: AbstractFactoryLandToRaw):
        pass

    @abstractmethod
    def process(self):
        pass

#IMPLEMENTATION FOR THE TEMPLATE
class LandToRawTemplate(InterfaceLandToRawTemplate):

    def __init__(self, catalog_name: str, schema_name: str, table_name: str, batch_id: str):

        self.catalog_name = catalog_name
        self.table_name = table_name
        self.schema_name = schema_name
        self.batch_id = batch_id

        self.table_reader = None
        self.governance_interactor = None
        self.asset_interactor = None

        self.dataframe = None
        self.metadata = None
        
        self.metrics = {}
        self.metrics['batch_id'] = batch_id
        self.metrics['loaded_at'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        self.metrics['initial_rows'] = None
        self.metrics['final_rows'] = None
        self.metrics['accumulated_rows'] = None

        self.dataframe_ingestion_metrics = None

    def set_component_factory(self, component_factory: AbstractFactoryLandToRaw):
        self.table_reader = component_factory.table_reader
        self.governance_interactor = component_factory.governance_interactor
        self.asset_interactor = component_factory.asset_interactor
  
    def process(self):

        print('Starting land to raw process ...')

        #EXTRACTION STEPS
        self.extract_metadata()
        self.extract_table()

        #PROCESSING STEPS
        self.validate_required_columns()
        self.cast_data_types()
        self.validate_non_nullable_columns()
        self.add_metadata()

        #EXPORTING STEPS
        self.write_table()
        self.get_accumulated_rows()
        self.generate_ingestion_metrics()
        self.write_metrics()

        print('Table, metadata and ingestion metrics exported successfully')

    #EXTRACT METADATA
    def extract_metadata(self):
     
        print('Extracting metadata ...')
        self.metadata = self.governance_interactor.get_table_metadata(catalog_name=self.catalog_name, 
                                                                      schema_name=self.schema_name, 
                                                                      table_name=self.table_name)

    #EXTRACT TABLE
    def extract_table(self):

        #EXTRACT LANDING FILE
        print('Extracting landing file ...')

        self.dataframe = self.table_reader.read_table()
        self.metrics['initial_rows'] = self.dataframe.count()

    #VALIDATE REQUIRED COLUMNS
    def validate_required_columns(self):

        print('Validating required columns ...')
        required_columns = self.metadata['field_data'].keys()
        dataframe_columns = self.dataframe.columns

        for column in required_columns:
            if column not in dataframe_columns:
                raise ValueError(f"Missing column in dataframe: {column}")
            
        for column in dataframe_columns:
            if column not in required_columns:
                raise ValueError(f"Missing column in metadata: {column}")

    #CAST DATA TYPES
    def cast_data_types(self):
        
        print('Casting data types ...')
        for column in self.metadata['field_data'].keys():

            plain_data_type = self.metadata['field_data'][column]['data_type']
            data_type = None

            if plain_data_type == 'string':
                data_type = StringType()
            elif plain_data_type == 'json':
                data_type = StringType()
            elif plain_data_type == 'timestamp':
                data_type = TimestampType()
            elif plain_data_type == 'integer':
                data_type = IntegerType()
            elif plain_data_type == 'boolean':
                data_type = BooleanType()
            else:
                raise ValueError(f"Invalid data type: {plain_data_type}")

            self.dataframe = self.dataframe.withColumn(column, self.dataframe[column].cast(data_type))

    #VALIDATE NON NULLABLE COLUMNS
    def validate_non_nullable_columns(self):

        print('Validating non nullable columns ...')
        for column in self.metadata['field_data'].keys():

            #NON NULLABLE
            if self.metadata['field_data'][column]['is_nullable'] == False:
                count_null = self.dataframe.filter(col(column).isNull()).count()

                if count_null > 0:
                    raise ValueError(f"Null value found in column: {column}") 

    #ADDING METADATA
    def add_metadata(self):

        #ADD COMMENTS
        print('Adding metadata ...')
        for column in self.metadata['field_data'].keys():
            self.dataframe = self.dataframe.withMetadata(column, {'comment': self.metadata['field_data'][column]['comment']})

        #ADD METADATA COLUMNS
        self.dataframe = self.dataframe.withColumn('metadata_batch_id', lit(self.metrics['batch_id']))
        self.dataframe = self.dataframe.withColumn('metadata_loaded_at', lit(self.metrics['loaded_at']))
        self.dataframe = self.dataframe.withColumn('metadata_etl_module', lit(self.metadata['etl_module']))

        self.dataframe = self.dataframe.withColumn('metadata_loaded_at', to_timestamp(col('metadata_loaded_at'), "yyyy-MM-dd HH:mm:ss"))

        self.dataframe = self.dataframe.withMetadata('metadata_batch_id', {'comment': 'Identifier of the batch process'})
        self.dataframe = self.dataframe.withMetadata('metadata_loaded_at', {'comment': 'Timestamp when the record was loaded into the table'})
        self.dataframe = self.dataframe.withMetadata('metadata_etl_module', {'comment': 'ETL module used to process this record'}) 

        self.metrics['final_rows'] = self.dataframe.count()

    #WRITE TABLE
    def write_table(self):

        print('Writing table ...')
        params = {'write_mode': self.metadata['write_mode'], 'batch_id': self.metrics['batch_id']}
        
        self.asset_interactor.write_table(dataframe=self.dataframe, 
                                          catalog_name=self.metadata['catalog_name'], 
                                          schema_name=self.metadata['schema_name'],
                                          table_name=self.metadata['table_name'],
                                          params = params)

    #GET ACCUMULATED ROWS
    def get_accumulated_rows(self):

        print('Computing accumulated rows for metrics ...')
        self.metrics['accumulated_rows'] = self.asset_interactor.get_total_rows(catalog_name=self.metadata['catalog_name'], 
                                                                                schema_name=self.metadata['schema_name'], 
                                                                                table_name=self.metadata['table_name'])

    #GENERATE INGESTION METRICS
    def generate_ingestion_metrics(self):

        print('Generating ingestion metrics ...')
        columns = ['table_id', 'catalog_name', 'schema_name', 'table_name', 'batch_id', 'loaded_at', 
                   'etl_module', 'write_mode', 'initial_rows', 'final_rows', 'accumulated_rows', 'quality']

        ingestion_metrics = [(self.metadata['table_id'],
                              self.metadata['catalog_name'], 
                              self.metadata['schema_name'], 
                              self.metadata['table_name'], 
                              self.metrics['batch_id'], 
                              self.metrics['loaded_at'], 
                              self.metadata['etl_module'], 
                              self.metadata['write_mode'], 
                              self.metrics['initial_rows'], 
                              self.metrics['final_rows'], 
                              self.metrics['accumulated_rows'],
                              self.metadata['quality'])]

        self.dataframe_ingestion_metrics = spark.createDataFrame(ingestion_metrics, columns)

    #WRITE INGESTION METRICS
    def write_metrics(self):

        print('Writing ingestion metrics ...')
        self.asset_interactor.merge_dataframe(self.dataframe_ingestion_metrics, catalog_name='governance_prod', 
                                              schema_name='metrics', table_name='ingestions', 
                                              match_columns=['catalog_name', 'schema_name', 'table_name', 'batch_id'])
    