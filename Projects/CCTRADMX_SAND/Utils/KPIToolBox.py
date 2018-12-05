
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
from KPIUtils_v2.DB.CommonV2 import Common

__author__ = 'ilays'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_COOLER_AVAILABILITY = 'Cooler_Availability'

class CCTRADMXToolBox:
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
        self.all_data = pd.merge(self.scif, self.match_product_in_scene[['bay_number','scene_fk']],
                                 how="inner", left_on='scene_id', right_on='scene_fk').drop_duplicates()

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.calculate_cooler_availability()
        self.common.commit_results_data()

    def calculate_cooler_availability(self):
        template_bays = self.all_data[['template_fk', 'additional_attribute_1',
                                       'additional_attribute_2', 'bay_number']].drop_duplicates()
        template_bays = template_bays[(template_bays['additional_attribute_2'].isin(['Frio', 'Fr'+u'\xed'+'o']))]
        if template_bays.empty:
            return
        template_bays_max = template_bays.groupby(['template_fk'], sort=False).max().reset_index()
        template_bays_max['number_of_scenes'] = 1
        template_bays_final_df = template_bays_max.groupby(['additional_attribute_1'], sort=False).sum().reset_index()

        try:
            atomic_pk = self.common.get_kpi_fk_by_kpi_type(KPI_COOLER_AVAILABILITY)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + KPI_COOLER_AVAILABILITY)
            return

        for index, row in template_bays_final_df.iterrows():
            cooler_type = row['additional_attribute_1']
            count_of_doors = row['bay_number']
            count_of_coolers = row['number_of_scenes']
            numerator_id = self.check_numerator_id(cooler_type)
            self.common.write_to_db_result(fk=atomic_pk, numerator_id=numerator_id,
                                                numerator_result=count_of_coolers, denominator_id=0,
                                                denominator_result=count_of_doors,result=count_of_coolers)

    def check_numerator_id(self, cooler_type):
        if cooler_type == 'KO':
            return 1
        elif cooler_type == 'Propio':
            return 2
        elif cooler_type == 'Competencia':
            return 3
        else:
            return 4


























