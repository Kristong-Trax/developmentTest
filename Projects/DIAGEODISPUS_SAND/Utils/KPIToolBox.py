import os
import pandas as pd
import numpy as np
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

__author__ = 'nissand'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class DIAGEODISPUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                 'kpi_template.xlsx')
    DISPLAY_CASES = 'Display Cases'
    DISPLAY_BOTTLES = 'Display Bottles'
    CASE_PACK = 'CASE PACK'
    SKU_PERFORMANCE = 'sku_performance'
    BRAND_PERFORMANCE = 'brand_performance'

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
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.new_kpi_static_data = self.common.get_new_kpi_static_data()
        self.kpi_results_queries = []
        self.template = self.occupancy_template = pd.read_excel(self.TEMPLATE_PATH)
        self.manual_collection_number = []

    def get_kpi_fk_by_type(self, kpi_type):
        assert isinstance(kpi_type, (unicode, basestring)), "name is not a string: %r" % kpi_type
        try:
            return self.new_kpi_static_data[self.kpi_static_data['type'] == kpi_type]['pk'].values[0]
        except IndexError:
            Log.info("Kpi name: {} is not equal to any kpi name in static table".format(kpi_type))
            return None


    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        relevant_brands = self.template['brand_name'].values
        relevant_products = self.scif[(self.scif['brand_name'].isin(relevant_brands)) & (self.scif['oos_sc'] != 1)]
        sku_results = pd.DataFrame(columns=['sku', 'brand', 'num_of_cases'])
        sku_results['sku'] = relevant_products
        for product in relevant_products:
            display_cases = self.manual_collection_number[self.manual_collection_number['product_fk'] == product][
                self.DISPLAY_CASES].drop_duplicates().values[0]
            display_bottles = self.manual_collection_number[self.manual_collection_number['product_fk'] == product][
                self.DISPLAY_BOTTLES].drop_duplicates().values[0]
            case_pack = self.scif[self.scif['product_fk'] == product][self.CASE_PACK].drop_duplicates().values[0]
            # sku_results.loc[sku_results['sku']== product, ['brand', 'num_of_cases']] = []
            sku_results.loc[sku_results['sku'] == product, 'brand'] = self.scif[self.scif['product_fk'] == product][
                'brand_fk'].drop_duplicates().values[0]
            sku_results.loc[sku_results['sku'] == product, 'num_of_cases'] = display_cases + np.divide(float(
                display_bottles), float(case_pack))
        sku_kpi_fk = self.get_kpi_fk_by_type(self.SKU_PERFORMANCE)
        for row in sku_results.itertuples():
            self.common.write_to_db_result_new_tables(fk=sku_kpi_fk, numerator_id=row.sku,
                                                      numerator_result=row.num_of_cases, result=row.num_of_cases,
                                                      score=row.num_of_cases)
        brand_results = pd.DataFrame(columns=['brand', 'num_of_cases'])
        brand_results['brand'] = sku_results['brand'].drop_duplicates().values
        for brand in sku_results['brand'].drop_duplicates().values:
            brand_results.loc[brand_results['brand'] == brand, 'num_of_cases'] = sku_results[sku_results['brand'] ==
                                                                                         brand]['num_of_cases'].sum()
        brand_kpi_fk = self.get_kpi_fk_by_type(self.BRAND_PERFORMANCE)
        for row in brand_results.itertuples():
            self.common.write_to_db_result_new_tables(fk=brand_kpi_fk, numerator_id=row.brand,
                                                      numerator_result=row.num_of_cases, result=row.num_of_cases,
                                                      score=row.num_of_cases)
        return
