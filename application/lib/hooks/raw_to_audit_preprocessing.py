import json
import pandas as pd

from abc import ABC, abstractmethod

import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

def convert_to_json_string(text):

    if pd.isnull(text):
        return None

    elements = text.split(',')
    for element in elements:
        if pd.isnull(element) or element == '':
            elements.remove(element)
    return json.dumps(elements)

convert_to_json_string_udf = udf(convert_to_json_string, StringType())


#INTERFACE FOR PREPROCESSIG STEPS IN THE RAW TO AUDIT PROCESS
class InterfaceHookRawToAuditPreprocessing(ABC):

    @abstractmethod
    def transform(self) -> DataFrame:
        pass

#HOOK FOR VISITS
class HookVisitsPreprocessing(InterfaceHookRawToAuditPreprocessing):

    def __init__(self):
        pass

    def transform(self, dataframe):

        columns = ['FechaOpen', 'FechaClick', 'Links', 'IPs', 'Navegadores', 'Plataformas']
        for column in columns:
            dataframe = dataframe.withColumn(column, regexp_replace(col(column), 'unknown', ''))
            dataframe = dataframe.withColumn(column, when(col(column) == '-', lit(None)).otherwise(col(column)))
            dataframe = dataframe.withColumn(column, when(col(column) == '', lit(None)).otherwise(col(column)))

        columns = ['Links', 'IPs', 'Navegadores', 'Plataformas']
        for column in columns:
            dataframe = dataframe.withColumn(column, convert_to_json_string_udf(col(column)))

        return dataframe