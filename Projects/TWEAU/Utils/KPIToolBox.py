
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.CommonV2 import Common, PSProjectConnector

import pandas as pd
import os
import math
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'sathiyanarayanan'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_FAMILY = 'kpi_family_fk'
PS_KPI_FAMILY = 19
TYPE = 'type'

# Template
KPI_SHEET  = 'KPI'
KPI_TYPE = 'kpi_type'
KPI_NAME = 'kpi_name'

NUMERATOR_FK = 'numerator_key'
DENOMINATOR_FK = 'denominator_key'
FILTER_ENTITIES = [1, 2, 3]

class TWEAUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'Template.xlsx')
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.calculate_macro_linear()
        score = 0
        return score

    def calculate_macro_linear(self):
        df_tp_ps_kpis = self.get_template_details(KPI_SHEET)

        for index, row in df_tp_ps_kpis.iterrows():
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == row[KPI_TYPE])
                                       & (self.kpi_static_data['delete_time'].isnull())]

            if kpi.empty:
                print("KPI Name:{} not found in DB".format(row[KPI_NAME]))
            else:
                print("KPI Name:{} found in DB".format(row[KPI_NAME]))
                # generate the numerator filter string
                numerator_filters = []
                numerator_filter_string = ''
                for each_idx in FILTER_ENTITIES:
                    numerator_filter = row['filter_entity_' + str(each_idx)]
                    if numerator_filter != numerator_filter:
                        # it is NaN
                        continue
                    numerator_filter_value = row['filter_entity_' + str(each_idx) + '_value']
                    numerator_filter_string += numerator_filter + "==" + '"' + numerator_filter_value + '"'
                    numerator_filters.append(numerator_filter)
                    numerator_filter_string += ' and '
                numerator_filter_string = numerator_filter_string.rstrip(' and')

                numerator_data_frame = pd.DataFrame(self.scif.query(numerator_filter_string)).fillna(0).\
                    groupby(numerator_filters, as_index=False).agg({'gross_len_add_stack': 'sum'})
                # numerator_len_total = 0
                # for idx, numerator_row in numerator_data_frame.iterrows():
                #     numerator_len_total += numerator_row.gross_len_add_stack



    def get_template_details(self, sheet_name):
        template = pd.read_excel(self.excel_file_path, sheet_name=sheet_name)
        return template
