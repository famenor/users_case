import pandas as pd

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.getActiveSession()


## THIS COMPONENT CONTAINS THE CODE MODULES FOR READS AND WRITES IN PLAIN FILES

#INTERFACE WITH METHODS THAT MUST BE DEFINED BY THE CONCRETE IMPLEMENTATIONS
class InterfaceTableReader(ABC):

    @abstractmethod
    def read_table(self) -> DataFrame:
        pass

#ABSTRACT CLASS WITH THE COMMON FILE METHODS IMPLEMENTED
class AbstractFileReader(InterfaceTableReader):

    def __init__(self, file_path: str):
        self.file_path = file_path

    @abstractmethod
    def read_table(self) -> DataFrame:
        pass

#CONCRETE IMPLEMENTATION FOR READING CSV FILES
class CsvFileReader(AbstractFileReader):

    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.separator = ','

    def set_separator(self, separator: str):
        self.separator = separator

    def read_table(self) -> DataFrame:
        return spark.read.format('csv').option('header', 'true').option('sep', self.separator).load(self.file_path)