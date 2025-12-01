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
from lib.interactor.validation import FacadeScreenValidator
from lib.interactor.surrogate_key import HashKeyInteractor
from lib.hooks.raw_to_audit_preprocessing import *

spark = SparkSession.getActiveSession()


## FACTORIES FOR COMPONENTS REQUIRED BY THE TEMPLATE

#ABTRACT FACTORY WITH METHODS TO BE IMPLEMENTED
class AbstractFactoryRawToAudit(ABC):

    @abstractmethod
    def create(self):
        pass

#CONCRETE FACTORY FOR VISITS
class FactoryRawToAuditForVisit(AbstractFactoryRawToAudit):

    def __init__(self):
        pass
        
    def create(self):

        database_gateway = SparkSQLDatabaseGateway()
  
        self.governance_interactor = GovernanceInteractor(database_gateway=database_gateway)
        self.asset_interactor = AssetInteractor(database_gateway=database_gateway)
        self.hash_key_interactor = HashKeyInteractor()
        self.facade_screen_validator = FacadeScreenValidator()
        self.hook_preprocessing = HookVisitsPreprocessing()


## TEMPLATE FOR EXTRACTING A TABLE AND METADATA, PROCESSING THE TABLE AND EXPORTING THE TABLE WITH METADATA AND METRICS

#INTERFACE FOR THE TEMPLETE
class InterfaceRawToAuditTemplate(ABC):

    #@abstractmethod
    def set_component_factory(self, component_factory: AbstractFactoryRawToAudit):
        pass

    @abstractmethod
    def process(self):
        pass

#IMPLEMENTATION FOR THE TEMPLATE
class RawToAuditTemplate(InterfaceRawToAuditTemplate):

    def __init__(self, catalog_name: str, schema_name: str, table_name: str, rundate: str, batch_id: str):

        self.catalog_name = catalog_name
        self.table_name = table_name
        self.schema_name = schema_name
        self.batch_id = batch_id

        self.governance_interactor = None
        self.asset_interactor = None
        self.facade_screen_validator = None
        self.hook_preprocessing = None

        self.dataframe = None
        self.dataframe_errors = None
        self.metadata = None
        
        self.metrics = {}
        self.metrics['rundate'] = rundate
        self.metrics['batch_id'] = batch_id
        self.metrics['loaded_at'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        self.metrics['initial_rows'] = None
        self.metrics['final_rows'] = None
        self.metrics['accumulated_rows'] = None

        self.dataframe_ingestion_metrics = None

    def set_component_factory(self, component_factory: AbstractFactoryRawToAudit):
        self.governance_interactor = component_factory.governance_interactor
        self.asset_interactor = component_factory.asset_interactor
        self.hash_key_interactor = component_factory.hash_key_interactor
        self.facade_screen_validator = component_factory.facade_screen_validator
        self.hook_preprocessing = component_factory.hook_preprocessing
  
    def process(self):

        print('Starting land to raw process ...')

        #EXTRACTION STEPS
        self.extract_metadata()
        self.extract_table()

        #PROCESSING STEPS
        self.rename_columns()
        self.apply_hook_preprocessing()
        self.validate_screens()
        self.cast_data_types()
        self.add_metadata()
        self.generate_errors_dataframe()
        self.discard_primary_key_errors()

        #EXPORTING STEPS
        self.write_table()
        self.get_accumulated_rows()
        self.generate_ingestion_metrics()
        self.write_metrics()
        self.write_errors()

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

        raw_schema_name = self.schema_name.replace('silver', 'bronze')
        raw_table_name = self.table_name.replace('audit', 'raw')
        params = {'read_mode': 'read_partition', 'batch_id': self.batch_id}

        self.dataframe = self.asset_interactor.read_table(catalog_name=self.catalog_name, 
                                                          schema_name=raw_schema_name, 
                                                          table_name=raw_table_name, 
                                                          params=params)
        
        self.dataframe = self.dataframe.drop('metadata_batch_id', 'metadata_loaded_at', 'metadata_etl_module')
        self.dataframe = self.dataframe.withColumn('metadata_audit_id', lit(None))
        self.dataframe = self.dataframe.withColumn('metadata_audit_passed', lit(True))
        self.dataframe = self.dataframe.withColumn('row_temp_id', monotonically_increasing_id())
      
        self.metrics['initial_rows'] = self.dataframe.count()

    #RENAME COLUMNS
    def rename_columns(self):

        print('Renaming columns ...')
        for column in self.metadata['field_data'].keys():
            
            rename_from = self.metadata['field_data'][column]['rename_from']
            self.dataframe = self.dataframe.withColumnRenamed(rename_from, column)

    #HOOK FOR CUSTOM PREPROCESSING
    def apply_hook_preprocessing(self):

        print('Preprocessing ...')
        self.dataframe = self.hook_preprocessing.transform(self.dataframe)

    #VALIDATE SCREENS
    def validate_screens(self):

        print('Validating screens ...')
        self.facade_screen_validator.set_field_data(self.metadata['field_data'])
        self.facade_screen_validator.screen(self.dataframe)

        self.dataframe = self.facade_screen_validator.dataframe
        self.dataframe_errors = self.facade_screen_validator.dataframe_errors

    #CAST DATA TYPES
    def cast_data_types(self):
        
        print('Casting data types ...')
        for column in self.metadata['field_data'].keys():

            plain_data_type = self.metadata['field_data'][column]['data_type']
            data_type = None
            date_format = None

            if plain_data_type == 'string':
                data_type = StringType()
            elif plain_data_type == 'json':
                data_type = StringType()
            elif plain_data_type == 'timestamp':
                data_type = TimestampType()
                date_format = self.facade_screen_validator.parsed_validations['is_date_format'][column]
            elif plain_data_type == 'integer':
                data_type = IntegerType()
            elif plain_data_type == 'boolean':
                data_type = BooleanType()
            else:
                raise ValueError(f"Invalid data type: {plain_data_type}")

            #CAST
            if data_type == TimestampType():
                self.dataframe = self.dataframe.withColumn(column, to_timestamp(self.dataframe[column], 
                                                                                date_format).cast(TimestampType()))                
            else:
                self.dataframe = self.dataframe.withColumn(column, self.dataframe[column].cast(data_type))

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

        self.dataframe = self.dataframe.withColumn('metadata_audit_id', when(col('metadata_audit_passed') == True, lit(1)).otherwise(lit(2)))
        self.dataframe = self.dataframe.withColumn('metadata_created_at', lit(None))
        self.dataframe = self.dataframe.withColumn('metadata_updated_at', lit(None))
        self.dataframe = self.dataframe.withColumn('metadata_deleted_at', lit(None))

        self.dataframe = self.dataframe.withColumn('metadata_created_at', col('metadata_created_at').cast(TimestampType()))
        self.dataframe = self.dataframe.withColumn('metadata_updated_at', col('metadata_updated_at').cast(TimestampType()))
        self.dataframe = self.dataframe.withColumn('metadata_deleted_at', col('metadata_deleted_at').cast(TimestampType()))

    #GENERATE ERRORS DATAFRAME
    def generate_errors_dataframe(self):

        print('Generating errors dataframe ...')
        self.dataframe_errors = self.dataframe_errors.withColumnRenamed('value', 'original_value')
        self.dataframe_errors = self.dataframe_errors.withColumn('batch_id', lit(self.metrics['batch_id']))
        self.dataframe_errors = self.dataframe_errors.withColumn('catalog_name', lit(self.metadata['catalog_name']))
        self.dataframe_errors = self.dataframe_errors.withColumn('schema_name', lit(self.metadata['schema_name']))
        self.dataframe_errors = self.dataframe_errors.withColumn('table_name', lit(self.metadata['table_name']))
        self.dataframe_errors = self.dataframe_errors.withColumn('replaced_value', lit(None))
        self.dataframe_errors = self.dataframe_errors.withColumn('error_condition', lit('Sin Resolver'))

        self.dataframe_errors = self.dataframe_errors.join(self.dataframe.select('row_temp_id', 'Email'), on='row_temp_id', how='inner')
        self.dataframe_errors = self.dataframe_errors.withColumnRenamed('Email', 'record_identifier')
        self.dataframe_errors = self.dataframe_errors.drop('row_temp_id')

        base_columns = ['catalog_name', 'schema_name', 'table_name', 'column_name', 'batch_id', 'record_identifier', 'screen_code']       
        self.dataframe_errors = self.hash_key_interactor.assign_hash_key(self.dataframe_errors, base_columns, 'error_event_id')
        self.dataframe = self.dataframe.drop('row_temp_id')

    #DISCARD PRIMARY KEY ERRORS
    def discard_primary_key_errors(self):

        print('Discarding primary key errors ...')
        pk_errors = self.dataframe_errors.filter(col('screen_code') == 'is_primary_key').select('record_identifier')
        pk_errors = pk_errors.withColumnRenamed('record_identifier', 'Email')

        self.dataframe = self.dataframe.join(pk_errors, on='Email', how='left_anti')
        self.metrics['final_rows'] = self.dataframe.count()

    #WRITE TABLE
    def write_table(self):

        print('Writing table ...')
        match_columns = ['Email', 'metadata_batch_id']
        altered_at = self.metrics['loaded_at']
        
        self.asset_interactor.merge_dataframe_with_altered_at(dataframe=self.dataframe, 
                                                              catalog_name=self.metadata['catalog_name'], 
                                                              schema_name=self.metadata['schema_name'],
                                                              table_name=self.metadata['table_name'],
                                                              match_columns=match_columns,
                                                              altered_at=altered_at)

    #GET ACCUMULATED ROWS
    def get_accumulated_rows(self):

        print('Computing accumulated rows for metrics ...')
        self.metrics['accumulated_rows'] = self.asset_interactor.get_total_rows(catalog_name=self.metadata['catalog_name'], 
                                                                                schema_name=self.metadata['schema_name'], 
                                                                                table_name=self.metadata['table_name'])

    #GENERATE INGESTION METRICS
    def generate_ingestion_metrics(self):

        print('Generating ingestion metrics ...')
        columns = ['table_id', 'catalog_name', 'schema_name', 'table_name', 'rundate', 'batch_id', 'loaded_at', 
                   'etl_module', 'write_mode', 'initial_rows', 'final_rows', 'accumulated_rows', 'quality']

        ingestion_metrics = [(self.metadata['table_id'],
                              self.metadata['catalog_name'], 
                              self.metadata['schema_name'], 
                              self.metadata['table_name'], 
                              self.metrics['rundate'],
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
        
    #WRITE INGESTION ERRORS
    def write_errors(self):

        self.dataframe_errors = self.dataframe_errors.dropDuplicates(subset=['error_event_id'])

        print('Writing ingestion errors ...')
        self.asset_interactor.merge_dataframe(self.dataframe_errors, catalog_name='governance_prod', 
                                              schema_name='metrics', table_name='event_errors', 
                                              match_columns=['error_event_id'])
