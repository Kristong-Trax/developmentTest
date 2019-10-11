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

KPI_TYPE = 'Type'

# Column Name
KPI_NAME = 'KPI Name'
PRODUCT_TYPE = 'product_type'
NUMERATOR_PARAM_1 = 'numerator param 1'
NUMERATOR_VALUE_1 = 'numerator value 1'
DENOMINATOR_PARAM_1 = 'denominator param 1'
DENOMINATOR_VALUE_1 = 'denominator value 1'
IGNORE_STACKING = 'Ignore Stacking'

# Sheet names
SOS = 'SOS'

Sheets = [SOS]
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'CC Nayar Template v0.3.xlsx')


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
        self.calculate_sos
        return

    @property
    def calculate_sos(self):
        for i, row in self.templates[SOS].iterrows():
            # Step 1 Read the excel rows to proces the information
            kpi_name = row["KPI Name"]
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

            product_type = self.sanitize_values(row[PRODUCT_TYPE])
            template_group = self.sanitize_values(row['Task/ Template Group'])

            ignore_stacking = row[IGNORE_STACKING]

            numerator_param1 = row[NUMERATOR_PARAM_1]
            numerator_value1 = self.sanitize_values(row[NUMERATOR_VALUE_1])

            denominator_param1 = row[DENOMINATOR_PARAM_1]
            denominator_value1 = row[DENOMINATOR_VALUE_1]

            # Step 2: Filter the self.scif by the columns required
            column_filter_for_scif = ['pk', 'session_id', 'template_group', PRODUCT_TYPE, 'facings',
                                      'facings_ign_stack'] + \
                                     self.delete_filter_nan([numerator_param1, denominator_param1])

            filtered_scif = self.scif[column_filter_for_scif]

            # Step 3: Filter each perticular column by the required value

            # 3A.Logic for considering stacking or ignore stacking
            if pd.isnull(ignore_stacking):
                relevant_scif = filtered_scif.drop(columns=['facings_ign_stack'])
                relevant_scif = relevant_scif.rename(columns={'facings': 'final_facings'})
            elif ignore_stacking == 'Y':
                relevant_scif = filtered_scif.drop(columns=['facings'])
                relevant_scif = relevant_scif.rename(columns={'facings_ign_stack': 'final_facings'})

            # 3B.Filters for the product type and template_group
            relevant_scif = relevant_scif[relevant_scif[PRODUCT_TYPE].isin(product_type)]

            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(template_group)]

            # 3C.Filter through the denominator param for the denominator value
            if pd.isnull(denominator_param1):
                denominator_scif = relevant_scif
            else:
                denominator_scif = relevant_scif[relevant_scif[denominator_param1].isin([denominator_value1])]

            # 3D.Filter through numerator param for the numerator value
            if pd.isnull(numerator_param1):
                numerator_scif = denominator_scif
            else:
                numerator_scif = denominator_scif[denominator_scif[numerator_param1].isin(numerator_value1)]

            # 4. Setting up numerator_entity and the denominator_entity

            # 4A. Find the numerator entity
            numerator_id = self.scif[row['Numerator Entity']].mode()[0]
            denominator_id = self.scif[row['Denominator Entity']].mode()[0]

            #Check with Hunter
            # if row['Numerator Entity'] == 'manufacturer_fk':
            #     numerator_id = self.scif.manufacturer_name.mode()[0]
            # elif row['Numerator Entity'] == 'brand_fk':
            #     numerator_id = self.scif.brand_name.mode()[0]
            #
            # if row['Denominator Entity'] == 'template_fk':
            #     denominator_id = self.scif.template_group.mode()[0]
            # elif row['Denominator Entity'] == 'manufacturer_fk':
            #     denominator_id = self.scif.manufacturer_name.mode()[0]
            #


            numerator_result = numerator_scif['final_facings'].sum()

            denominator_result = denominator_scif['final_facings'].sum()
            result = numerator_result / denominator_result



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
