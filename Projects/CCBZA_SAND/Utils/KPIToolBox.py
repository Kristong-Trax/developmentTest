import pandas as pd
import os
import re

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.CCBZA_SAND.Utils.Fetcher import CCBZA_SAND_Queries
from Projects.CCBZA_SAND.Utils.ParseTemplates import parse_template

__author__ = 'natalyak'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

# Template tabs
KPI_TAB = 'KPI'
SET_TAB = 'Set'
PLANOGRAM_TAB = 'Planogram'
PRICE_TAB = 'Price'
SURVEY_TAB = 'Survey'
AVAILABILITY_TAB = 'Availability'
SOS_TAB = 'SOS'
COUNT_TAB = 'Count'

# Template columns
SET_NAME = 'Set Name'
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
SPLIT_SCORE = 'Split Score'
DEPENDENCY = 'Dependency'
ATOMIC_KPI_NAME = 'Atomic KPI Name'
TARGET = 'Target'
SCORE = 'Score'
STORE_TYPE = 'Store Type'
ATTRIBUTE_1 = 'Attribute_1'
ATTRIBUTE_2 = 'Attribute_2'
EXPECTED_RESULT = 'Expected Result'
SURVEY_QUESTION_CODE = 'Survey Q Code'
TEMPLATE_NAME = 'Template Name'
TEMPLATE_GROUP = 'Template Group'

#Other constants

class CCBZA_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.store_data = self.get_store_data_by_store_id()
        self.template_path = self.get_template_path()
        self.template_data = self.get_template_data()

    def main_calculation(self, kpi_set_fk, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # if self.template_data:
        score = 0
        kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
        kpi_types = self.get_kpi_types_by_set_name(kpi_set_name)
        kpi_data = self.template_data[KPI_TAB][self.template_data[KPI_TAB][SET_NAME] == kpi_set_name]
        for index, kpi in kpi_data.iterrows():
            for kpi_type in kpi_types:
                atomic_kpis_data = self.get_atomic_kpis_data(kpi_type, kpi)


        return score

    def get_store_data_by_store_id(self):
        query = CCBZA_SAND_Queries.get_store_data_by_store_id(self.store_id)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_template_path(self):
        store_type = self.store_data['store_type'].values[0]      # to check
        print store_type
        template_name = 'Template_{}.xlsx'.format(store_type)
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', template_name)

    def get_template_data(self):
        sheet_names = pd.ExcelFile(self.template_path).sheet_names
        template_data = {}
        for sheet in sheet_names:
            template_data[sheet] = parse_template(self.template_path, sheet)
        return template_data

    def get_relevant_template_data(self):
        pass

    def get_kpi_types_by_set_name(self, kpi_set_name):
        kpi_types = self.template_data[SET_TAB][self.template_data[SET_TAB][KPI_NAME] == kpi_set_name][KPI_TYPE].values[0]
        # kpi_types_list = kpi_types.split(', ') if ', ' in kpi_types else kpi_types.split(',')
        kpi_types_list = re.split(r', |,| ,', kpi_types)
        return kpi_types_list

    def get_atomic_kpis_data(self, kpi_type, kpi):
        atomic_kpis_data = self.template_data[kpi_type][(self.template_data[kpi_type][KPI_NAME] == kpi[KPI_NAME]) &
                                                        (self.template_data[kpi_type][STORE_TYPE].isin(self.store_data['store_type']))  &
                                                        (self.template_data[kpi_type][ATTRIBUTE_1].isin(self.store_data['additional_attribute_1'])) &
                                                        (self.template_data[kpi_type][ATTRIBUTE_2].isin(self.store_data['additional_attribute_2']))]
        return atomic_kpis_data
        

    # def get_kpi_data_by_set_name(self, kpi_set_name):
    #     kpi_data = self.template_data[KPI_TAB][self.template_data[KPI_TAB][SET_NAME] == kpi_set_name]

