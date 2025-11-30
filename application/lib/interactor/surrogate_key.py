import pandas as pd

from abc import ABC, abstractmethod
from lib.gateway.database import InterfaceDatabaseGateway

import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


## THIS COMPONENT CONTAINS THE CODE MODULES FOR INTERACTORS WITH SURROGATE KEYS

#INTERFACE WITH METHODS THAT MUST BE DEFINED BY THE CONCRETE IMPLEMENTATIONS 
class InterfaceSurrogateKeyInteractor(ABC):

    @abstractmethod
    def assign_surrogate_key(self) -> int:
        pass

class InterfaceHashKeyInteractor(ABC):

    @abstractmethod
    def assign_hash_key(self, DataFrame, base_columns: list, target_column: str) -> DataFrame:
        pass

#USE CASES FOR SURROGATE KEYS
class SurrogateKeyInteractor(InterfaceSurrogateKeyInteractor):

    def __init__(self, database_gateway: InterfaceDatabaseGateway):
        self.database_gateway = database_gateway

    #ASSING A SURROGATE KEY TO A RECORD CONSIDERING BASE COLUMNS
    def assign_surrogate_key(self, catalog_name, schema_name, table_name, base_values, surrogate_column):

        surrogate_id = self.database_gateway.get_surrogate_id(catalog_name=catalog_name, 
                                                              schema_name=schema_name, 
                                                              table_name=table_name, 
                                                              base_values=base_values,
                                                              surrogate_column=surrogate_column)

        if pd.isnull(surrogate_id):

            new_surrogate_id = None
            current_max_value = self.database_gateway.get_max_value(catalog_name=catalog_name, 
                                                                   schema_name=schema_name, 
                                                                   table_name=table_name, 
                                                                   column=surrogate_column)

            if pd.isnull(current_max_value):
                new_surrogate_id = 1

            else:
                new_surrogate_id = current_max_value + 1

            return new_surrogate_id

        else:
            return surrogate_id
        
class HashKeyInteractor(InterfaceHashKeyInteractor):

    def __init__(self):
        pass

    #ASSING A HASH KEY TO A RECORD CONSIDERING BASE COLUMNS
    def assign_hash_key(self, dataframe: DataFrame, base_columns: list, target_column: str) -> DataFrame:

        base = []
        for column in base_columns:
            base.append(coalesce(col(column), lit('')))

        dataframe = dataframe.withColumn('__temp_combined_columns__',
            concat_ws("||", *base) 
        ).withColumn(target_column, md5(col('__temp_combined_columns__'))).drop('__temp_combined_columns__')

        return dataframe


