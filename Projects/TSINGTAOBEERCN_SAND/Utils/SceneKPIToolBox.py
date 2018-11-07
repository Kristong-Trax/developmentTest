from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox


import pandas as pd

__author__ = 'ilays'


class SceneToolBox:

    def __init__(self, data_provider, common):
        self.data_provider = data_provider
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

        self.calculate_facings_per_sku(location_type_1=True)
        self.calculate_facings_per_sku(location_type_1=False)


    def calculate_facings_per_sku(self, location_type_1):
        if location_type_1:
            self.count_facings_by_scenes(self.scif, {'location_type':(self.tools.INCLUDE_FILTER), "product_type":"SKU"})
        else:
            self.count_facings_by_scenes(self.scif, {'location_type':(self.tools.EXCLUDE_FILTER), "product_type":"SKU"})

        # self.common.write_to_db_result(fk=atomic_pk, numerator_id=1, identifier_parent=parent_kpi,
        #                                   numerator_result=numerator_number_of_facings, denominator_id=self.store_fk,
        #                                   denominator_result_after_actions=3, should_enter=True,
        #                                   denominator_result=denominator_number_of_total_facings, result=count_result)

    def count_facings_by_scenes(self, df, filters):
        facing_data = df[self.tools.get_filter_condition(df, **filters)]
        # filter by scene_id and by template_name (scene type)
        scene_types_groupby = facing_data.groupby(['scene_id'])['facings'].sum().reset_index()
        return scene_types_groupby