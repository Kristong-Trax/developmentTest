
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
import numpy as np
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class HEINEKENTWToolBox:
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
        self.assortment = Assortment(self.data_provider, self.output)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.assortment_calculation()
        self.common.commit_results_data_to_new_tables()

    def assortment_calculation(self):
        """
        This function calculates 3 levels of assortment :
        level3 is assortment SKU
        level2 is assortment groups
        level1 how many groups passed out of all
        """
        lvl3_result = self.assortment.calculate_lvl3_assortment()

        for result in lvl3_result.itertuples():
            score = result.in_store
            self.common.write_to_db_result_new_tables(result.kpi_fk_lvl3, result.product_fk, result.in_store,
                                                      score, result.assortment_group_fk, 1, score)
        if not lvl3_result.empty:
            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                res = np.divide(float(result.passes), float(denominator_res))
                if result.passes >= 1:
                    score = 100
                else:
                    score = 0
                self.common.write_to_db_result_new_tables(result.kpi_fk_lvl2, result.assortment_group_fk,
                                                          result.passes,
                                                          res, result.assortment_super_group_fk, denominator_res,
                                                          score)

        return

