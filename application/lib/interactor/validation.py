import json
import pandas as pd
from abc import ABC, abstractmethod

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.getActiveSession()


class ValidationParser():

    def __init__(self, field_data):

        self.field_data = field_data

        self.parsed_validations = {}
        self.parsed_validations['is_primary_key'] = []
        self.parsed_validations['is_not_null'] = []
        self.parsed_validations['unique'] = {}
        self.parsed_validations['is_in_list'] = {}
        self.parsed_validations['is_in_foreign_column'] = {}
        self.parsed_validations['is_date_format'] = {}
        self.parsed_validations['is_email_format'] = []
        self.parsed_validations['is_in_bounds'] = {}
        self.parsed_validations['is_not_lower_than'] = {}

    def process(self):

        for column in self.field_data.keys():
            
            is_primary_key = self.field_data[column]['is_primary_key']
            validations = self.field_data[column]['validations']
            data_type = self.field_data[column]['data_type']

            if is_primary_key == True:
                self.parsed_validations['is_primary_key'].append(column)

            if pd.isnull(validations):
                continue

            validations = json.loads(validations)
            for i in range(0, len(validations)):

                if validations[i]['validation'] == 'is_not_null':
                    self.parsed_validations['is_not_null'].append(column)

                elif validations[i]['validation'] == 'unique':
                    unique_group = validations[i]['unique_group']

                    if unique_group in parsed_validations['unique'].keys():
                        self.parsed_validations['unique'][unique_group].append(column)
                    else:
                        self.parsed_validations['unique'][unique_group] = [column]

                elif validations[i]['validation'] == 'is_in_list':

                    if 'allowed' in validations[i].keys():
                        self.parsed_validations['is_in_list'][column] = validations[i]['allowed'].split(',')

                    elif 'reference' in validations[i].keys():
                        self.parsed_validations['is_in_foreign_column'][column] = {'schema': validations[i]['schema'], 
                                                                                   'table': validations[i]['table'], 
                                                                                   'reference': validations[i]['reference']}
                        
                elif validations[i]['validation'] == 'is_date_format':
                    self.parsed_validations['is_date_format'][column] = validations[i]['format']

                elif validations[i]['validation'] == 'is_email_format':
                    self.parsed_validations['is_email_format'].append(column)

                elif validations[i]['validation'] == 'is_in_bounds':
                    self.parsed_validations['is_in_bounds'][column] = {'min_allowed': validations[i]['min_allowed'], 
                                                                       'max_allowed': validations[i]['max_allowed'],
                                                                       'data_type': data_type}
                    
                elif validations[i]['validation'] == 'is_not_lower_than':
                    self.parsed_validations['is_not_lower_than'][column] = {'reference': validations[i]['reference'],
                                                                            'data_type': data_type}
                    
        return self.parsed_validations
            
class InterfaceScreenValidator(ABC):

    @abstractmethod
    def filter(self):
        pass

    @abstractmethod
    def get_validation_code(self) -> str:
        pass

    @abstractmethod
    def validate(self):
        pass

class AbstractScreenValidator(InterfaceScreenValidator):

    def __init__(self, dataframe, column, dataframe_errors):
        self.dataframe = dataframe
        self.column = column
        self.dataframe_errors = dataframe_errors
        self.validation_code = self.get_validation_code()

    @abstractmethod
    def filter(self):
        pass

    @abstractmethod
    def get_validation_code(self) -> str:
        pass

    def validate(self):

        print('Validating column: ' + self.column + ' with screen ' + self.validation_code)
        
        fails = self.filter()
        fails = fails.select('row_temp_id', self.column)
        fails = fails.withColumnRenamed(self.column, 'value')
        fails = fails.withColumn('screen_code', lit(self.validation_code))
        fails = fails.withColumn('column_name', lit(self.column))
        fails = fails.withColumn('value', col('value').cast(StringType()))

        self.dataframe_errors = self.dataframe_errors.union(fails)

        fails_list = fails.select('row_temp_id').collect()
        fails_list = [getattr(row, 'row_temp_id') for row in fails_list]

        self.dataframe = self.dataframe.withColumn('metadata_audit_passed', when(col('row_temp_id').isin(fails_list), 
                                                                                 False).otherwise(col('metadata_audit_passed')))
        
        if self.validation_code == 'is_primary_key':
            pass
        else:
            self.dataframe = self.dataframe.withColumn(self.column, when(col('row_temp_id').isin(fails_list), lit(None)).otherwise(col(self.column)))

        return self.dataframe, self.dataframe_errors
    
class IsPrimaryKeyScreenValidator(AbstractScreenValidator):

    def filter(self):

        count = self.dataframe.groupBy(self.column).count()
        count = count.filter(col('count') > 1).select(self.column)

        return self.dataframe.join(count, self.column, 'inner')

    def get_validation_code(self):
        return 'is_primary_key'

class IsNotNullScreenValidator(AbstractScreenValidator):

    def filter(self):
        return self.dataframe.where(col(self.column).isNull()).select('row_temp_id', self.column)

    def get_validation_code(self):
        return 'is_not_null'
    
class IsInListScreenValidator(AbstractScreenValidator):

    def set_allowed_values(self, allowed_values):
        self.allowed_values = allowed_values

    def filter(self):
        return self.dataframe.where(~col(self.column).isin(self.allowed_values))

    def get_validation_code(self):
        return 'is_in_list'
    
class IsEmailFormatScreenValidator(AbstractScreenValidator):

    def filter(self):
        return self.dataframe.where(~regexp_extract(col(self.column), r'^.+@.+\..+$', 0).cast('string').isNotNull())

    def get_validation_code(self):
        return 'is_email_format'
    
class IsInBoundsScreenValidator(AbstractScreenValidator):

    def set_bounds(self, min_allowed, max_allowed):
        self.min_allowed = min_allowed
        self.max_allowed = max_allowed

    def filter(self):
        return self.dataframe.where((col(self.column) < self.min_allowed) | (col(self.column) > self.max_allowed))

    def get_validation_code(self):
        return 'is_in_bounds'

class IsDateFormatScreenValidator(AbstractScreenValidator):

    def set_format(self, format):
        self.format = format

    def filter(self):

        fails = self.dataframe.withColumn('temp', when((try_to_date(col(self.column), self.format).isNotNull()) |
                                                                   (col(self.column).isNull()), True).otherwise(False))
        return fails.where((col('temp') == False))
    
    def get_validation_code(self):
        return 'is_date_format'   

class FacadeScreenValidator():

    def __init__(self):

        self.dataframe = None

        schema = StructType([
            StructField("row_temp_id", IntegerType(), False),
            StructField("value", StringType(), True),
            StructField("screen_code", StringType(), False),
            StructField("column_name", StringType(), False)
        ])

        self.dataframe_errors = spark.createDataFrame([], schema)

    def set_field_data(self, field_data):

        validation_parser = ValidationParser(field_data)
        self.parsed_validations = validation_parser.process()
        
    def screen(self, dataframe):

        self.dataframe = dataframe

        #IS PRIMARY KEY SCREEN
        for column in self.parsed_validations['is_primary_key']:

            screen_validator = IsPrimaryKeyScreenValidator(self.dataframe, column, self.dataframe_errors)
            self.dataframe, self.dataframe_errors = screen_validator.validate()

        #IS NOT NULL SCREEN
        for column in self.parsed_validations['is_not_null']:  

            screen_validator = IsNotNullScreenValidator(self.dataframe, column, self.dataframe_errors)
            self.dataframe, self.dataframe_errors = screen_validator.validate()

        #IS IN LIST SCREEN
        for column in self.parsed_validations['is_in_list']:

            screen_validator = IsInListScreenValidator(self.dataframe, column, self.dataframe_errors)
            screen_validator.set_allowed_values(self.parsed_validations['is_in_list'][column])
            self.dataframe, self.dataframe_errors = screen_validator.validate()

        #IS EMAIL FORMAT
        for column in self.parsed_validations['is_email_format']:
            
            screen_validator = IsEmailFormatScreenValidator(self.dataframe, column, self.dataframe_errors)
            self.dataframe, self.dataframe_errors = screen_validator.validate()

        #IS IN BOUNDS SCREEN
        for column in self.parsed_validations['is_in_bounds']:

            screen_validator = IsInBoundsScreenValidator(self.dataframe, column, self.dataframe_errors)
            screen_validator.set_bounds(self.parsed_validations['is_in_bounds'][column]['min_allowed'], 
                                        self.parsed_validations['is_in_bounds'][column]['max_allowed'])
            self.dataframe, self.dataframe_errors = screen_validator.validate()

        #IS DATE FORMAT SCREEN
        for column in self.parsed_validations['is_date_format']:

            screen_validator = IsDateFormatScreenValidator(self.dataframe, column, self.dataframe_errors)
            screen_validator.set_format(self.parsed_validations['is_date_format'][column])
            self.dataframe, self.dataframe_errors = screen_validator.validate()      