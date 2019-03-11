
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.Common import Common as CommonV1
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'natalyak'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class PEPSICOUKToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUSION_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                      'Incl_Excl_Template.xlsx')

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common_v1 = CommonV1(self.data_provider)
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
        self.incl_excl_template = self.get_exclusion_template_data()

        self.assortment = Assortment(self.data_provider, self.output, common=self.common_v1)
        self.lvl3_ass_result = self.assortment.calculate_lvl3_assortment()

#------------------init functions-----------------
    def get_exclusion_template_data(self):
        return pd.read_excel(self.EXCLUSION_TEMPLATE_PATH)

#------------------utility functions--------------

#------------------main project calculations------

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates and writes to DB the KPI results.
        """
        self.calculate_external_kpis()
        self.calculate_internal_kpis()

    def calculate_external_kpis(self):
        self.calculate_assortment()
        self.calculate_linear_sos_hero_sku()
        self.calculate_linear_sos_brand()
        self.calculate_linear_sos_sub_brand()
        self.calculate_linear_sos_segment()
        pass

    def calculate_assortment(self):
        pass

    def calculate_linear_sos_hero_sku(self):
        pass

    def calculate_linear_sos_brand(self):
        pass

    def calculate_linear_sos_sub_brand(self):
        pass

    def calculate_linear_sos_segment(self):
        pass

    def calculate_internal_kpis(self):
        pass
