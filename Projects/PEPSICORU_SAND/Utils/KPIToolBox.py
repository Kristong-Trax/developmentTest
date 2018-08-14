import os
import pandas as pd
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log
from Projects.PEPSICORU_SAND.Utils.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import  GENERALToolBox

__author__ = 'idanr'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class PEPSICORUToolBox:

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
        self.sos_results = {}


        self.categories_to_calculate = [Const.SNACKS, Const.BEVERAGES, Const.JUICES] #TODO a function.
        self.sos = SOS(data_provider, output, self.rds_conn)
        self.assortment = Assortment(self.data_provider, self.output, common=self.common)
        self.general_tool_box = GENERALToolBox(data_provider)

    @log_runtime('Share of shelf pepsicoRU')
    def calculate_share_of_shelf(self):
        """
        This function filters
        :return:
        """
        filtered_df = self.scif
        locations = self.scif[Const.TEMPLATE_GROUP].unique().tolist() #todo: change template group?
        for location in locations:
            filter_loc_param = {Const.TEMPLATE_GROUP: location}
            filtered_by_loc = filtered_df[self.general_tool_box.get_filter_condition(filtered_df, **filter_loc_param)]
            facings_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_LOCATION)
            linear_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_LOCATION)
            # self.sos_results[facings_kpi_fk] = [numerator, denominator]
            linear_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_LOCATION)

            for category in self.categories_to_calculate:
                filter_cat_param = {Const.CATEGORY: category}
                filtered_df_by_cat = filtered_by_loc[
                    self.general_tool_box.get_filter_condition(filtered_by_loc, **filter_cat_param)]
                facings_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_CATEGORY)
                linear_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_CATEGORY)
            for sub_cat in self.scif[Const.SUB_CATEGORY].unique().tolist():
                filter_sub_cat_param = {Const.SUB_CATEGORY: sub_cat}
                filtered_df_by_sub_cat = filtered_by_loc[
                    self.general_tool_box.get_filter_condition(filtered_by_loc, **filter_sub_cat_param)]
                facings_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_SUB_CATEGORY)
                linear_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_SUB_CATEGORY)
            for brand in self.scif[Const.BRAND_NAME]:
                filter_brand_param = {Const.BRAND_NAME: brand}
                filtered_df_by_sub_cat = filtered_by_loc[
                    self.general_tool_box.get_filter_condition(filtered_by_loc, **filter_brand_param)]
                facings_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_BRAND)
                linear_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_BRAND)

        return


    def get_relevant_session_categories(self):
        """
        This functions returns the relevant categories to calculate according to the current scene types
        :return: list of the categories that needs to be calculate
        """
        # todo
        # Filter scif???
        return [Const.SNACKS]

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.assortment_calculation()
        session_categories = self.get_relevant_session_categories()
        score = 0
        return score
