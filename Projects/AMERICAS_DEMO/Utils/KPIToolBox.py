
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'yoava'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class AMERICASToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    FIRST_KPI = 'First KPI'
    SECOND_KPI = 'Second KPI'

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

    def get_product_fk_from_product_name(self, product_name):
        return self.all_products.product_fk[self.all_products.product_name == product_name].values[0]

    def calculate_first_kpi(self):
        """
        this method if in shelf number 2 all products are the product name below
        :return: True/False
        """
        product_name = 'CORONA LIGHT 355ML BOTE TALLCAN'
        mpis = self.match_product_in_scene
        second_shelf = mpis[mpis.shelf_number == 2]
        unique_products_in_second_self = second_shelf.product_fk.unique()
        if len(unique_products_in_second_self) != 1:
            return False
        else:
            product_fk = self.get_product_fk_from_product_name(product_name)
            if product_fk in unique_products_in_second_self:
                return True
            else:
                return False

    def calculate_second_kpi(self):
        product_name = 'MODELO ESPECIAL 355ML BOTE SINGLE'
        product_fk = self.get_product_fk_from_product_name(product_name)
        products_in_scene_df = self.match_product_in_scene[self.match_product_in_scene.product_fk == product_fk]
        facing_counter = 0
        for i, row in products_in_scene_df.iterrows():
            if str(row.face_count) == 'nan':
                facing_counter += 1
            else:
                facing_counter += row.face_count
        return facing_counter >= 5

    def write_result_to_db(self, kpi_name, kpi_result):
        if kpi_result:
            score = 100
        else:
            score = 0
        atomic_kpi_fk = self.kpi_static_data.atomic_kpi_fk[self.kpi_static_data.atomic_kpi_name == kpi_name].values[0]
        self.common.write_to_db_result(atomic_kpi_fk, self.LEVEL3, score)
        kpi_fk = self.kpi_static_data.kpi_fk[self.kpi_static_data.kpi_name == kpi_name].values[0]
        self.common.write_to_db_result(kpi_fk,self.LEVEL2, score)
        kpi_set_fk = self.kpi_static_data.kpi_set_fk[self.kpi_static_data.kpi_name == kpi_name].values[0]
        self.common.write_to_db_result(kpi_set_fk, self.LEVEL1, score)

    def main_calculation(self):
        self.write_result_to_db(self.FIRST_KPI, self.calculate_first_kpi())
        self.write_result_to_db(self.SECOND_KPI, self.calculate_second_kpi())
        self.common.commit_results_data()

# asdasdasdad