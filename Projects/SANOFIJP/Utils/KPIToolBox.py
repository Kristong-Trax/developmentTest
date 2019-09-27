from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data

PS_KPI_FAMILY = 19
TYPE = 'type'
KPI_FAMILY = 'kpi_family_fk'
ALL_PROD_KPI = 'SIMON_ALL_PRODUCTS'

__author__ = 'nidhin'


class SanofiJPToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.kpi_static_data = self.common.get_kpi_static_data()

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.record_all_products()
        # self.common.commit_results_data()  # [PROS-11697] No need as its already committed by the Global KPIs
        return 0

    def record_all_products(self):
        kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                   & (self.kpi_static_data[TYPE] == ALL_PROD_KPI)
                                   & (self.kpi_static_data['delete_time'].isnull())]
        if kpi.empty:
            Log.info("KPI Name:{} not found in DB".format(ALL_PROD_KPI))
        else:
            Log.info("KPI Name:{} found in DB".format(ALL_PROD_KPI))
        num_of_prods = 0
        # it will have a substitution  product; where the facings are aggregated
        self.scif.dropna(subset=['facings'], inplace=True)
        for index, each_row in self.scif.iterrows():
            self.common.write_to_db_result(
                fk=int(kpi.iloc[0].pk),
                numerator_id=int(each_row.product_fk),
                numerator_result=int(each_row.facings),
                denominator_id=int(self.store_id),
                denominator_result=1,
                result=int(each_row.scene_id),
                score=1,
                context_id=int(each_row.template_fk),
            )
            num_of_prods += 1
        Log.info("{proj} - For session: {sess}, {prod_count} products were written for kpi: {kpi_name}".format(
            proj=self.project_name,
            sess=self.session_uid,
            prod_count=num_of_prods,
            kpi_name=kpi.iloc[0].type,
        ))
