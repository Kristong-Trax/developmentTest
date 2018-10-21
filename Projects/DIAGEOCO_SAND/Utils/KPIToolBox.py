
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils.DB.Common import Common
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

        # the manufacturer name for DIAGEO is 'Diageo' by default. We need to redefine this for DiageoCO
        self.global_gen.tool_box.DIAGEO = 'DIAGEO'

        self.calculate_block_together() # working
        self.calculate_secondary_display() # working
        self.calculate_brand_pouring_status() # working
        # self.calculate_touch_point() # using old tables, needs work
        self.calculate_relative_position() # working
        # self.calculate_activation_standard() # using old tables, needs work

        self.global_gen.diageo_global_assortment_function() # working
        # self.global_gen.diageo_global_share_of_shelf_function() # need template

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
        # needs work

    def calculate_block_together(self):
        results_list = self.global_gen.diageo_global_block_together(Const.BRAND_BLOCKING_BRAND_FROM_CATEGORY, self.brand_blocking_template)
        if results_list:
            for result in results_list:
                self.common.write_to_db_result_new_tables(**result)

    def calculate_activation_standard(self):
        result = self.global_gen.diageo_global_activation_standard_function(kpi_scores, set_scores, local_templates_path)
        # needs work

    def calculate_relative_position(self):
        # returns list of dict
        results_list = self.global_gen.diageo_global_relative_position_function(self.relative_positioning_template)
        if results_list:
            for result in results_list:
                self.common.write_to_db_result_new_tables(**result)

    def commit_results_data(self):
        print('success')
        # self.common.commit_results_data_to_new_tables()
