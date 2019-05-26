
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from Trax.Utils.Logging.Logger import Log

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class GSKAUToolBox:
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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'), sheet_name='Functional KPIs',
                                             keep_default_na=False)

        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        assortment_store_dict = self.gsk_generator.availability_store_function()
        self.common.save_json_to_new_tables(assortment_store_dict)

        assortment_category_dict = self.gsk_generator.availability_category_function()
        self.common.save_json_to_new_tables(assortment_category_dict)

        assortment_subcategory_dict = self.gsk_generator.availability_subcategory_function()
        self.common.save_json_to_new_tables(assortment_subcategory_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_whole_store_function()
        self.common.save_json_to_new_tables(facings_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_whole_store_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_sub_category_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_by_sub_category_function()
        self.common.save_json_to_new_tables(facings_sos_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function()
        self.common.save_json_to_new_tables(facings_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_category_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        self.common.commit_results_data()

        return
