
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

import pandas as pd

__author__ = 'ilays'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

ASSORTMENT_KPI = 'ASSORTMENT_ON_SKU_LEVEL'
SESSION_SKU_FACINGS_KPI = 'FACINGS_PER_SKU_SESSION'

class TSINGTAOBEERCNToolBox:
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
        self.assortment = self.data_provider[Data.ASSORTMENTS]
        self.tools = GENERALToolBox(data_provider)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calculate_sku_facing_session_level()
        self.calculate_facings_by_assortment()
        self.common.commit_results_data()

    def calculate_sku_facing_session_level(self):
        filters = {"product_type": ["SKU", "Other"]}
        result_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)][['product_fk', 'facings']]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(SESSION_SKU_FACINGS_KPI)
        for index, row in result_df.iterrows():
            result = row['facings']
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=row['product_fk'], denominator_id=self.store_id,
                                           score=result, result=result)

    def calculate_facings_by_assortment(self):
        assortment_scif_merge = pd.merge(self.assortment, self.scif, how="left",
                                    on=['template_fk', 'product_fk'])[['product_fk', 'template_fk', 'facings']]
        result_df = assortment_scif_merge.groupby(['product_fk'])['facings'].sum().reset_index()
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(ASSORTMENT_KPI)
        for index, row in result_df.iterrows():
            result = 1 if row['facings'] else 0
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=row['product_fk'], denominator_id=self.store_id,
                                            score=result, result=result, numerator_result=row['facings'])


