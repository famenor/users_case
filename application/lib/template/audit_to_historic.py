import pandas as pd
from abc import ABC, abstractmethod

import datetime
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


## FACTORIES FOR COMPONENTS REQUIRED BY THE TEMPLATE

#ABTRACT FACTORY WITH METHODS TO BE IMPLEMENTED
class AbstractFactoryAuditToHistoric(ABC):

    @abstractmethod
    def create(self):
        pass

#CONCRETE FACTORY FOR VISITS
class FactoryAuditToHistoric(AbstractFactoryAuditToHistoric):

    def __init__(self):
        pass
        
    def create(self):

        database_gateway = SparkSQLDatabaseGateway()
  
        self.governance_interactor = GovernanceInteractor(database_gateway=database_gateway)
        self.asset_interactor = AssetInteractor(database_gateway=database_gateway)
        self.hash_key_interactor = HashKeyInteractor()


## TEMPLATE FOR EXTRACTING A TABLE AND METADATA, PROCESSING THE TABLE AND EXPORTING THE TABLE WITH METADATA AND METRICS

#INTERFACE FOR THE TEMPLETE
class InterfaceAuditToHistoricTemplate(ABC):

    #@abstractmethod
    def set_component_factory(self, component_factory: AbstractFactoryAuditToHistoric):
        pass

    @abstractmethod
    def process(self):
        pass

#IMPLEMENTATION FOR THE TEMPLATE
class AuditToHistoricTemplate(InterfaceAuditToHistoricTemplate):

    def __init__(self, catalog_name: str, schema_name: str, table_name: str):

        self.catalog_name = catalog_name
        self.table_name = table_name
        self.schema_name = schema_name

        self.governance_interactor = None
        self.asset_interactor = None
        self.hash_key_interactor = None

        self.dataframe = None
        self.metadata = None
        self.dataframe_historic = None
        
        self.metrics = {}
        self.metrics['batch_id'] = 'unique'
        self.metrics['loaded_at'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        self.metrics['initial_rows'] = None
        self.metrics['final_rows'] = None
        self.metrics['accumulated_rows'] = None

        self.dataframe_ingestion_metrics = None

    def set_component_factory(self, component_factory: AbstractFactoryAuditToHistoric):
        self.governance_interactor = component_factory.governance_interactor
        self.asset_interactor = component_factory.asset_interactor
        self.hash_key_interactor = component_factory.hash_key_interactor
  
    def process(self):

        print('Starting land to raw process ...')

        #EXTRACTION STEPS
        self.extract_metadata()
        self.extract_table()

        #PROCESSING STEPS
        self.generate_history()

        #EXPORTING STEPS
        self.write_table()
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
        print('Extracting audited file ...')

        audit_schema_name = self.schema_name
        audit_table_name = self.table_name.replace('historic', 'audit')
        params = {'read_mode': 'read_audit_passed'}

        self.dataframe = self.asset_interactor.read_table(catalog_name=self.catalog_name, 
                                                          schema_name=audit_schema_name, 
                                                          table_name=audit_table_name, 
                                                          params=params)
        
        self.dataframe = self.dataframe.drop('metadata_batch_id', 'metadata_loaded_at', 'metadata_etl_module')  
        self.dataframe = self.dataframe.drop('metadata_audit_id', 'metadata_audit_passed')
        self.dataframe = self.dataframe.drop('metadata_created_at', 'metadata_updated_at', 'metadata_deleted_at') 

    #GENERATE HISTORY
    def generate_history(self):

        print('Consolidating historic changes ...')

        #EXTRACT OR CREATE HISTORIC TABLE
        self.primary_key = 'Email'
        self.valid_from = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        self.valid_to = datetime.datetime.strptime('2200-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")

        try:
            original_dataframe = spark.read.table(f'{self.metadata['catalog_name']}.{self.metadata['schema_name']}.{self.metadata['table_name']}')
            original_dataframe.count()

        except:
            new_original_dataframe = self.dataframe
            new_original_dataframe = new_original_dataframe.withColumn('is_current', lit(True))
            new_original_dataframe = new_original_dataframe.withColumn('valid_from', lit(self.valid_from))
            new_original_dataframe = new_original_dataframe.withColumn('valid_to', lit(self.valid_to))

            new_original_dataframe.write.format('delta').mode('overwrite').saveAsTable(f'{self.metadata['catalog_name']}.{self.metadata['schema_name']}.{self.metadata['table_name']}')

        #READ CURRENT HISTORIC FILE
        array_history_columns = []
        array_source_columns = []

        for column in self.metadata['field_data']:
            if self.metadata['field_data'][column]['track_changes'] == True:
                array_history_columns.append(column)
                array_source_columns.append('source_' + column)

        original_dataframe = spark.read.table(f'{self.metadata['catalog_name']}.{self.metadata['schema_name']}.{self.metadata['table_name']}')
        self.metrics['initial_rows'] = original_dataframe.count()

        new_dataframe = self.dataframe.select([col(column).alias('source_' + column) for column in self.dataframe.columns])

        new_dataframe_2 = new_dataframe
        new_dataframe_2 = new_dataframe_2.withColumn('source_is_current', lit(True))
        new_dataframe_2 = new_dataframe_2.withColumn('source_valid_from', lit(self.valid_from))
        new_dataframe_2 = new_dataframe_2.withColumn('source_valid_to', lit(self.valid_to))

        source_primary_key = 'source_' + self.primary_key
        merge_dataframe = original_dataframe.join(new_dataframe_2, (new_dataframe_2[source_primary_key] == original_dataframe[self.primary_key]), how='fullouter') 

        merge_dataframe = merge_dataframe.withColumn('concat_ws', concat_ws('+', *array_history_columns))
        merge_dataframe = merge_dataframe.withColumn('concat_ws_source', concat_ws('+', *array_source_columns))

        merge_dataframe = merge_dataframe.withColumn('action', when(concat_ws('+', *array_history_columns) == \
                                                                    concat_ws('+', *array_source_columns), 'NO_ACTION')
                                                        .when(merge_dataframe['is_current'] == False, 'NO ACTION')
                                                        .when(merge_dataframe[source_primary_key].isNull() & merge_dataframe.is_current, 'DELETE')
                                                        .when(merge_dataframe[source_primary_key].isNull(), 'INSERT')
                                                        .otherwise('UPDATE'))
        
        print('Rows with no action: ' + str(merge_dataframe.filter(col('action') == 'NO_ACTION').count()))
        print('Rows to insert: ' + str(merge_dataframe.filter(col('action') == 'INSERT').count()))
        print('Rows to update: ' + str(merge_dataframe.filter(col('action') == 'UPDATE').count()))
        print('Rows to delete: ' + str(merge_dataframe.filter(col('action') == 'DELETE').count()))

        array_history_columns = array_history_columns + ['is_current', 'valid_from', 'valid_to']
        array_source_columns = array_source_columns + ['source_is_current', 'source_valid_from', 'source_valid_to']

        #RECORDS WITH NO ACTION
        df_merge_part_01 = merge_dataframe.filter(merge_dataframe.action == 'NO_ACTION').select(array_history_columns)

        #RECORDS TO INSERT
        df_merge_part_02A = merge_dataframe.filter(merge_dataframe.action == 'INSERT').select(array_source_columns)
        df_merge_part_02B = df_merge_part_02A.select([col(column).alias(column.replace('source_', '')) for column in df_merge_part_02A.columns])

        #RECORDS TO DELETE
        df_merge_part_03 = merge_dataframe.filter(merge_dataframe.action == 'DELETE').select(array_history_columns)
        df_merge_part_03 = df_merge_part_03.withColumn('is_current', lit(False))
        df_merge_part_03 = df_merge_part_03.withColumn('valid_to', lit(self.valid_to))

        #RECORDS TO EXPIRE AND INSERT
        df_merge_part_04A = merge_dataframe.filter(merge_dataframe.action == 'UPDATE').select(array_source_columns)
        df_merge_part_04B = df_merge_part_04A.select([col(column).alias(column.replace('source_', '')) for column in df_merge_part_02A.columns])

        df_merge_part_04C = merge_dataframe.filter(merge_dataframe.action == 'UPDATE')
        df_merge_part_04C = df_merge_part_04C.withColumn('valid_to', merge_dataframe['source_valid_from'])
        df_merge_part_04C = df_merge_part_04C.withColumn('is_current', lit(False))
        df_merge_part_04C = df_merge_part_04C.select(array_history_columns)

        #UNION
        self.dataframe_historic = df_merge_part_01.unionAll(df_merge_part_02A).unionAll(df_merge_part_03).unionAll(df_merge_part_04B).unionAll(df_merge_part_04C)

        count = self.dataframe_historic.count()
        self.metrics['final_rows'] = count
        self.metrics['accumulated_rows'] = count

    #ADDING METADATA
    def add_metadata(self):
        pass

    #WRITE TABLE
    def write_table(self):

        print('Writing table ...')
        self.dataframe_historic.write.format('delta').mode('overwrite').saveAsTable(f'{self.metadata['catalog_name']}.{self.metadata['schema_name']}.{self.metadata['table_name']}')

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


