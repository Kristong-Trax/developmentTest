from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

import pandas as pd
import numpy as np
import os
from datetime import datetime

from Projects.CCNAYARMX.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'krishnat'

# Column Name
KPI_NAME = 'KPI Name'
TASK_TEMPLATE_GROUP = 'Task/ Template Group'
PRODUCT_TYPE = 'product_type'
NUMERATOR_PARAM_1 = 'numerator param 1'
NUMERATOR_VALUE_1 = 'numerator value 1'
DENOMINATOR_PARAM_1 = 'denominator param 1'
DENOMINATOR_VALUE_1 = 'denominator value 1'
IGNORE_STACKING = 'Ignore Stacking'
NUMERATOR_ENTITY = 'Numerator Entity'
DENOMINATOR_ENTITY = 'Denominator Entity'

# Sheet names
SOS = 'SOS'
BLOCK_TOGETHER = 'Block Together'
SHARE_OF_EMPTY = 'Share Of Empty'

# Scif Filters
BRAND_FK = 'brand_fk'
FACINGS = 'facings'
FACINGS_IGN_STACK = 'facings_ign_stack'
FINAL_FACINGS = 'final_facings'
MANUFACTURER_FK = 'manufacturer_fk'
PK = 'pk'
SESSION_ID = 'session_id'
TEMPLATE_FK = 'template_fk'
TEMPLATE_GROUP = 'template_group'

# Read the sheet
Sheets = [SOS, BLOCK_TOGETHER, SHARE_OF_EMPTY]

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'CCNayarTemplatev0.4 .xlsx')


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.ps_data_provider = PsDataProvider(data_provider)
        self.templates = {}
        self.parse_template()
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        a =1
    def parse_template(self):
        for sheet in Sheets:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        for i, row in self.templates[SHARE_OF_EMPTY].iterrows():
            # self.calculate_sos(row)
            # self.calculate_block_together(row)
            self.calculate_share_of_empty(row)


        return

    def calculate_sos(self, row):
        # Table of Contents:
        # Step 1 to 2: Declaring all the relevant columns from Sheet SOS
        # Step 3 to 5: Filtering scif to get all the relevant columns
        # Step 6 to 9: Filtering the relevant columns with the relevant rows; the id and result of numerator and denominator were calculated
        # Step 10: Calculates the final result
        # Step 11: Saves to the DB


        # Step 1: Read the excel rows to process the information (Common among all the sheets)
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        numerator_param1 = row[NUMERATOR_PARAM_1]
        numerator_value1 = row[NUMERATOR_VALUE_1]
        denominator_param1 = row[DENOMINATOR_PARAM_1]
        denominator_value1 = row[DENOMINATOR_VALUE_1]

        # Step 2: Import values that unique to the sheet SOS
        product_type = self.sanitize_values(row[PRODUCT_TYPE])
        ignore_stacking = row[IGNORE_STACKING]
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        # Step 3: Filter the self.scif by the columns required
        column_filter_for_scif = [PK, SESSION_ID, TEMPLATE_GROUP, PRODUCT_TYPE, FACINGS, FACINGS_IGN_STACK] + \
                                 [numerator_entity, denominator_entity] + \
                                 self.delete_filter_nan([numerator_param1, denominator_param1])

        # Step 4: Apply the filters to scif
        filtered_scif = self.scif[column_filter_for_scif]

        # Step 5: Determing where to use the facings or facings ignore stack column
        if pd.isna(ignore_stacking):
            filtered_scif = filtered_scif.drop(columns=[FACINGS_IGN_STACK])
            filtered_scif = filtered_scif.rename(columns={FACINGS: FINAL_FACINGS})
        elif ignore_stacking == 'Y':
            filtered_scif = filtered_scif.drop(columns=[FACINGS])
            filtered_scif = filtered_scif.rename(columns={FACINGS_IGN_STACK: FINAL_FACINGS})


        # Step 6: Filter through product type for relevant rows
        relevant_scif = filtered_scif[filtered_scif[PRODUCT_TYPE].isin(product_type)]

        # Step 7: Filter through template group for relevant rows
        relevant_scif = relevant_scif[relevant_scif[TEMPLATE_GROUP].isin(template_group)]

        # Step 8: Filter through the denominator param column with the denominator value and find the denominator result and id.
        if pd.isna(denominator_param1):
            denominator_scif = relevant_scif
            denominator_id = denominator_scif[denominator_entity].mode()[0]
            denominator_result = denominator_scif[FINAL_FACINGS].sum()
        else:
            denominator_scif = relevant_scif[relevant_scif[denominator_param1] == denominator_value1]
            denominator_id = \
                self.all_products[self.all_products[denominator_param1] == denominator_value1][
                    denominator_entity].mode()[0]

            denominator_result = denominator_scif[FINAL_FACINGS].sum()

        # Step 9: Filter through the numerator param column with numerator value and find the numerator result and id.
        if pd.isna(numerator_param1):
            numerator_scif = denominator_scif
            numerator_result = numerator_scif[FINAL_FACINGS].sum()
        else:
            # Sometimes the filter below overfilters, so the if statement was put in place
            if (denominator_scif[denominator_scif[numerator_param1] == numerator_value1]).empty: # Checks if the numerator_scif is empty
                numerator_id = self.scif[numerator_entity].mode()[0]
                numerator_result = 0
            else:
                numerator_scif = denominator_scif[denominator_scif[numerator_param1] == numerator_value1]
                numerator_id = \
                    self.all_products[self.all_products[numerator_param1] == numerator_value1][numerator_entity].mode()[
                        0]
                numerator_result = numerator_scif[FINAL_FACINGS].sum()

        # Step 10: Calculate the final result
        result = (numerator_result / denominator_result) * 100

        # Step 11. Save the results in the database
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       numerator_result=numerator_result, denominator_id=denominator_id,
                                       denominator_result=denominator_result,
                                       result=result)

    def calculate_block_together(self, row):

        # Step 1: Initialize the relevent columns from the Excel

        # Returns common columns acro
        self.standard_values_across_sheets()

        product_type = self.sanitize_values(row[PRODUCT_TYPE])
        ignore_stacking = row[IGNORE_STACKING]

        numerator_entity = row[NUMERATOR_ENTITY]

        denominator_entity = row[DENOMINATOR_ENTITY]

    def calculate_share_of_empty(self, row):
        # Table of Contents
        # Step 1 to 2: Declaring all the relevant columns from Sheet SOS
        # Step 3 to 6: Filtering scif to get all the relevant columns
        # Step 7 to 9: Calculating the Numerator Scif and Denominator Scif, and finding the results and id of both
        # Step 10: Calculates the results
        # Step 11: Saves to the database


        # Step 1: Read the excel rows to process the information (Common among all the sheets)
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        numerator_param1 = row[NUMERATOR_PARAM_1]
        numerator_value1 = row[NUMERATOR_VALUE_1]
        denominator_param1 = row[DENOMINATOR_PARAM_1]
        denominator_value1 = row[DENOMINATOR_VALUE_1]

        # Step 2: Import values that unique to the sheet SOS
        ignore_stacking = row[IGNORE_STACKING]
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]


        # Step 3: Filter the self.scif by the columns required
        column_filter_for_scif = [PK, SESSION_ID, TEMPLATE_GROUP, FACINGS, FACINGS_IGN_STACK] + \
                                 [numerator_entity, denominator_entity] + \
                                 self.delete_filter_nan([numerator_param1, denominator_param1])

        # Step 4: Apply the filters to scif
        filtered_scif = self.scif[column_filter_for_scif]

        # Step 5: Determing where to use the facings or facings ignore stack column
        if pd.isna(ignore_stacking):
            filtered_scif = filtered_scif.drop(columns=[FACINGS_IGN_STACK])
            filtered_scif = filtered_scif.rename(columns={FACINGS: FINAL_FACINGS})
        elif ignore_stacking == 'Y':
            filtered_scif = filtered_scif.drop(columns=[FACINGS])
            filtered_scif = filtered_scif.rename(columns={FACINGS_IGN_STACK: FINAL_FACINGS})

        # Step 6: Filtering the relevant columns with the relevant rows
        relevant_scif = filtered_scif[filtered_scif[TEMPLATE_GROUP].isin(template_group)]

        # Step 7: Filter through the denominator param column with the denominator value and calculate the result and id
        # For the KPI 31: Empty Denominator Param
        denominator_scif = relevant_scif
        denominator_result = relevant_scif[FINAL_FACINGS].sum()
        denominator_id = denominator_scif[denominator_entity].mode()[0]


        # Step 8: Filter through the numerator param column with numerator value and calculate the numerator result
        if denominator_scif[denominator_scif[numerator_param1] == numerator_value1].empty:
            numerator_result = 0
        else:
            numerator_scif = denominator_scif[denominator_scif[numerator_param1] == numerator_value1]
            numerator_result = numerator_scif[FINAL_FACINGS].sum()


        # Step 9: Find the numerator_id
        numerator_id = self.own_manuf_fk

        # Step 10: Calculate the result
        result = numerator_result/denominator_result

        # Step 11. Save the results in the database
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       numerator_result=numerator_result, denominator_id=denominator_id,
                                       denominator_result=denominator_result,
                                       result=result)


    def sanitize_values(self, item):
        if pd.isna(item):
            return item
        else:
            items = [x.strip() for x in item.split(',')]
            return items

    def delete_filter_nan(self, filters):
        filters = [filter for filter in filters if str(filter) != 'nan']
        return filters
