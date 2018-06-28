import os
import pandas as pd
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

# from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nissand'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class CCITToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                 'KPI_Templates.xlsx')
    CCIT_MANU = 'HBC Italia'
    MULTIPLIER_SHEET = 'Multiplier'
    STORE_ATT_1 = 'Store Att1'
    SCORE_MULTIPLIER = 'Score multiplier'
    NON_KPI = 0


    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.scene_results = self.ps_data_provider.get_scene_results(self.scene_info['scene_fk'].drop_duplicates().values)
        self.kpi_results_queries = []
        self.multiplier_template = pd.read_excel(self.TEMPLATE_PATH, sheetname=self.MULTIPLIER_SHEET)


    def get_manufacturer_fk(self, manu):
        return self.all_products[self.all_products['manufacturer_name'] ==
                                 manu]['manufacturer_fk'].drop_duplicates().values[0]


    def main_function(self):
        """
        This function calculates the KPI results.
        """
        relevant_kpi_res = self.common.get_kpi_fk_by_kpi_type('scene_score')
        scene_kpi_fks = self.scene_results[self.scene_results['kpi_level_2_fk'] == relevant_kpi_res]['pk'].values
        origin_res = self.scene_results[self.scene_results['kpi_level_2_fk'] == relevant_kpi_res]['result'].sum()
        # store_att_1 = self.store_info['additional_attribute_1'].values[0]
        # multiplier = self.multiplier_template[self.multiplier_template[self.STORE_ATT_1] == store_att_1][
        #     self.SCORE_MULTIPLIER]
        # multi_res = origin_res
        # if not multiplier.empty:
        #     multi_res = origin_res * multiplier.values[0]
        manu_fk = self.get_manufacturer_fk(self.CCIT_MANU)
        kpi_fk = self.common.get_kpi_fk_by_kpi_type('store_score')
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
        identifier_result['session_fk'] = self.session_info['pk'].values[0]
        identifier_result['store_fk'] = self.store_id
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=manu_fk, numerator_result=origin_res,
                                       result=origin_res, score=origin_res, should_enter=False,
                                       identifier_result=identifier_result)
        for scene in scene_kpi_fks:
            self.common.write_to_db_result(fk=self.NON_KPI, should_enter=True, scene_result_fk=scene,
                                           identifier_parent=identifier_result)
        return
