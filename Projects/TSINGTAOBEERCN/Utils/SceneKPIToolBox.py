from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

__author__ = 'ilays'

FACINGS_PER_SKU_SCENE_LOCATION_1_KPI = 'FACINGS_PER_SKU_SCENE_LOCATION_1'
FACINGS_PER_SKU_SCENE_LOCATION_OTHER_KPI = 'FACINGS_PER_SKU_SCENE_LOCATION_OTHER'

class SceneToolBox:

    def __init__(self, data_provider, common):
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.kpi_results_queries = []
        self.tools = GENERALToolBox(data_provider)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """

        self.calculate_facings_per_sku(location_type_1=True, kpi_name=FACINGS_PER_SKU_SCENE_LOCATION_1_KPI)
        self.calculate_facings_per_sku(location_type_1=False, kpi_name=FACINGS_PER_SKU_SCENE_LOCATION_OTHER_KPI)


    def calculate_facings_per_sku(self, location_type_1, kpi_name):
        if location_type_1:
            result_df = self.count_facings_by_scenes(self.scif, {'location_type_fk':(1, self.tools.INCLUDE_FILTER),
                                                                "product_type":["SKU","Other"]})[['product_fk', 'facings']]
        else:
            result_df = self.count_facings_by_scenes(self.scif, {'location_type_fk':(1, self.tools.EXCLUDE_FILTER),
                                                                "product_type":["SKU","Other"]})[['product_fk', 'facings']]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name=kpi_name)
        for index, row in result_df.iterrows():
            result = row['facings']
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=row['product_fk'], result=result, by_scene=True,
                                                                                        denominator_id=self.store_id)

    def count_facings_by_scenes(self, df, filters):
        facing_data = df[self.tools.get_filter_condition(df, **filters)]
        # filter by scene_id and by template_name (scene type)
        # scene_types_groupby = facing_data.groupby(['scene_id'])['facings'].sum().reset_index()
        return facing_data