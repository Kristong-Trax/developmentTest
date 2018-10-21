
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Projects.DIAGEOCO_SAND.Utils.Const import Const

import pandas as pd

__author__ = 'huntery'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

class DIAGEOCO_SANDToolBox:
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
        self.global_gen = DIAGEOGenerator(self.data_provider, self.output, self.common)

    def main_calculation(self, *args, **kwargs):

        """
        This function calculates the KPI results.
        """
        self.relative_positioning_template = pd.read_excel(Const.TEMPLATE_PATH, Const.RELATIVE_POSITIONING_SHEET_NAME,
                                                           header=Const.RELATIVE_POSITIONING_HEADER_ROW).to_dict(
            orient='records')
        self.brand_blocking_template = pd.read_excel(Const.TEMPLATE_PATH, Const.BRAND_BLOCKING_SHEET_NAME,
                                                     header=Const.BRAND_BLOCKING_HEADER_ROW).to_dict(orient='records')
        self.brand_pouring_status_template = pd.read_excel(Const.TEMPLATE_PATH, Const.BRAND_POURING_SHEET_NAME,
                                                           header=Const.BRAND_POURING_HEADER_ROW).to_dict(orient='records')

        # self.calculate_block_together()
        # self.calculate_secondary_display()
        self.calculate_brand_pouring_status()
        # self.calculate_touch_point()
        # self.calculate_relative_position()
        # self.calculate_activation_standard()

        # self.global_gen.diageo_global_assortment_function()
        # self.global_gen.diageo_global_share_of_shelf_function()

    def calculate_secondary_display(self):
        result = self.global_gen.diageo_global_secondary_display_secondary_function()
        if result:
            self.common.write_to_db_result_new_tables(**result)

    def calculate_brand_pouring_status(self):
        results_list = self.global_gen.diageo_global_brand_pouring_status_function(self.brand_pouring_status_template)
        if results_list:
            for result in results_list:
                self.common.write_to_db_result_new_tables(**result)

    def calculate_touch_point(self):
        result = self.global_gen.diageo_global_touch_point_function(self.touchpoint_template_path)
        print result

    def calculate_block_together(self):
        results_list = self.global_gen.diageo_global_block_together(Const.BRAND_BLOCKING_BRAND_FROM_CATEGORY, self.brand_blocking_template)
        if results_list:
            for result in results_list:
                self.common.write_to_db_result_new_tables(**result)

    def calculate_activation_standard(self):
        result = self.global_gen.diageo_global_activation_standard_function(kpi_scores, set_scores, local_templates_path)
        print result

    def calculate_relative_position(self):
        # returns list of dict
        results_list = self.global_gen.diageo_global_relative_position_function(self.relative_positioning_template)
        if results_list:
            for result in results_list:
                self.common.write_to_db_result_new_tables(**result)

    def commit_results_data(self):
        self.common.commit_results_data_to_new_tables()
