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
Sheets = [SOS, BLOCK_TOGETHER]
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

    def parse_template(self):
        for sheet in Sheets:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        for i, row in self.templates[BLOCK_TOGETHER].iterrows():
            # self.calculate_sos(row)
            self.calculate_block_together(row)
        return

    def calculate_sos(self, row):

        # Step 1 Read the excel rows to process the information
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        product_type = self.sanitize_values(row[PRODUCT_TYPE])
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        ignore_stacking = row[IGNORE_STACKING]

        numerator_param1 = row[NUMERATOR_PARAM_1]
        numerator_value1 = self.sanitize_values(row[NUMERATOR_VALUE_1])
        numerator_entity = row[NUMERATOR_ENTITY]

        denominator_param1 = row[DENOMINATOR_PARAM_1]
        denominator_value1 = self.sanitize_values(row[DENOMINATOR_VALUE_1])
        denominator_entity = row[DENOMINATOR_ENTITY]

        # Step 2: Filter the self.scif by the columns required
        column_filter_for_scif = [PK, SESSION_ID, TEMPLATE_GROUP, PRODUCT_TYPE, FACINGS,
                                  FACINGS_IGN_STACK] + \
                                 self.delete_filter_nan([numerator_param1, denominator_param1]) + \
                                 [row[NUMERATOR_ENTITY], row[DENOMINATOR_ENTITY]]

        filtered_scif = self.scif[column_filter_for_scif]

        # Step 3: Filter each perticular column by the required value

        # 3A.Logic for considering stacking or ignore stacking
        if pd.isnull(ignore_stacking):
            relevant_scif = filtered_scif.drop(columns=[FACINGS_IGN_STACK])
            relevant_scif = relevant_scif.rename(columns={FACINGS: FINAL_FACINGS})
        elif ignore_stacking == 'Y':
            relevant_scif = filtered_scif.drop(columns=[FACINGS])
            relevant_scif = relevant_scif.rename(columns={FACINGS_IGN_STACK: FINAL_FACINGS})

        # 3B.Filters for the product type and template_group
        final_relevant_scif = relevant_scif[relevant_scif[TEMPLATE_GROUP].isin(template_group)]

        # 3C.Filter through the denominator param for the denominator value
        if pd.isnull(denominator_param1):
            denominator_scif = final_relevant_scif
        else:
            denominator_scif = final_relevant_scif[final_relevant_scif[denominator_param1].isin(denominator_value1)]

        # 3D.Filter through numerator param for the numerator value
        if pd.isnull(numerator_param1):
            numerator_scif = denominator_scif
        else:
        # Deal with this. It can be null.
        # numerator_scif = denominator_scif[denominator_scif[numerator_param1].isin(numerator_value1)]

        # 4. Setting up numerator_id and denominator_id

        # 4A. Find the denominator_id
        if pd.isnull(denominator_param1):
            denominator_id = denominator_scif[denominator_entity].mode()[0]
        else:
            denominator_id = \
                self.all_products[self.all_products[denominator_param1] == denominator_value1[0]][
                    denominator_entity].mode()[0]

        # 4B. Find the numerator_id
        if pd.isnull(numerator_param1) or numerator_scif.empty:
            numerator_id = self.scif[numerator_entity].mode()[0]
        else:
            numerator_id = \
                self.all_products[self.all_products[numerator_param1] == numerator_value1[0]][numerator_entity].mode()[
                    0]

        numerator_result = numerator_scif[FINAL_FACINGS].sum()
        denominator_result = denominator_scif[FINAL_FACINGS].sum()
        result = (numerator_result / denominator_result) * 100
        a = 1

        # 5. Save the results in the database
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       numerator_result=numerator_result, denominator_id=denominator_id,
                                       denominator_result=denominator_result,
                                       result=result)

    def calculate_block_together(self, row):

        # Step 1: Initialize the relevent columns from the Excel

        #Returns common columns acro
        self.standard_values_across_sheets()

        product_type = self.sanitize_values(row[PRODUCT_TYPE])
        ignore_stacking = row[IGNORE_STACKING]


        numerator_entity = row[NUMERATOR_ENTITY]

        denominator_entity = row[DENOMINATOR_ENTITY]

    def sanitize_values(self, item):
        if pd.isna(item):
            return item
        else:
            items = [x.strip() for x in item.split(',')]
            return items

    def delete_filter_nan(self, filters):
        filters = [filter for filter in filters if str(filter) != 'nan']
        return filters

    def standard_values_across_sheets(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])

        numerator_param1 = row[NUMERATOR_PARAM_1]
        numerator_value1 = self.sanitize_values(row[NUMERATOR_VALUE_1])

        denominator_param1 = row[DENOMINATOR_PARAM_1]
        denominator_value1 = self.sanitize_values(row[DENOMINATOR_VALUE_1])

        return kpi_name, kpi_fk, template_group, numerator_param1, numerator_value1, denominator_param1, denominator_value1
