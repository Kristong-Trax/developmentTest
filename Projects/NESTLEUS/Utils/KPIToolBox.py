
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class NESTLEUSToolBox:
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
        self.new_kpi_static_data = self.common.get_new_kpi_static_data()
        self.kpi_results_queries = []
        self.linear_calc = SOS(self.data_provider)
        self.availability = Availability(self.data_provider)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        kpi_set_fk = kwargs['kpi_set_fk']
        # self.Calculate_facings_count(kpi_set_fk = kpi_set_fk)
        # self.Calculate_Linear_feet(kpi_set_fk = kpi_set_fk)
        # self.common.commit_results_data()
        pass







    def Calculate_facings_count(self, kpi_set_fk= None):
        if kpi_set_fk == 135:
            product_fks = self.all_products['product_fk'][
                (self.all_products['category_fk'] == 32) | (self.all_products['category_fk'] == 5)]
            for product_fk in product_fks:
                sos_filter = {'product_fk': product_fk}


                facing_count = self.availability.calculate_availability(**sos_filter)

                if facing_count > 0:
                    self.common.write_to_db_result(fk=kpi_set_fk, numerator_id=product_fk,
                                           numerator_result=facing_count,
                                           denominator_id=product_fk,
                                           result=facing_count, score=facing_count)


    def Calculate_Linear_feet(self, kpi_set_fk= None):
        if kpi_set_fk == 136:
            product_fks = self.all_products['product_fk'][(self.all_products['category_fk'] == 32) |  (self.all_products['category_fk'] == 5)]
            for product_fk in product_fks:
                sos_filter = {'product_fk':product_fk}
                general_filter = {'category_fk': [32,5]}

                ratio, numerator_length, denominator_length = self.linear_calc.calculate_linear_share_of_shelf_with_numerator_denominator(sos_filter, **general_filter)

                numerator_length = int(round(numerator_length * 0.0393700787))
                if numerator_length > 0:
                    self.common.write_to_db_result(fk=kpi_set_fk, numerator_id=product_fk, numerator_result=numerator_length,
                                               denominator_id=product_fk,
                                               result=numerator_length, score=numerator_length)

