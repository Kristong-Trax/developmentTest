from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data

import Projects.RINIELSENUS.TYSON.Utils.Const as Const

__author__ = 'Trevaris'

COLUMNS = ['product_fk', 'product_name', 'bay_number', 'scene_fk_x', 'brand_fk', 'brand_name',
           'manufacturer_fk', 'manufacturer_name', 'category_fk', 'category']


class TysonToolBox:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common = Common(data_provider)
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]

        self.mpis = self.match_product_in_scene.merge(
            self.scif, on='product_fk', how='left'
        )[COLUMNS].rename(columns={'scene_fk_x': 'scene_fk'})

    def main_calculation(self):
        """
        """

        self.calculate_shelf_neighbors('scrambles', 'breakfast meat')
        self.calculate_shelf_neighbors('scrambles', 'irrelevant')
        self.calculate_shelf_neighbors('ore ida', 'breakfast meat')
        self.calculate_shelf_neighbors('ore ida', 'irrelevant')

    def calculate_shelf_neighbors(self, target, neighbor):
        """

        """
        kpi = Const.KPIs[(target, neighbor)]
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi)
        brand_id = Const.BRANDs[target]
        result = self.neighbors(target, neighbor, 'product') if neighbor == 'irrelevant' \
            else self.neighbors(target, neighbor)

        self.common.write_to_db_result(
            fk=kpi_id,
            numerator_id=brand_id,
            numerator_result=result,
            denominator_id=self.store_id,
            denominator_result=1,
            result=result
        )

    def neighbors(self, target, neighbor, neighbor_type='category'):
        """

        """
        target_ids = Const.PRODUCT_IDS[target]
        neighbor_ids = Const.CATEGORIES.values() if neighbor_type == 'category' else Const.PRODUCT_IDS[neighbor]

        products = self.filter_df(self.mpis, 'product_fk', target_ids).drop_duplicates()
        categories = self.filter_df(self.mpis, neighbor_type+'_fk', neighbor_ids).drop_duplicates()
        neighbors = products.merge(categories, how='inner', on=['scene_fk', 'bay_number'])

        return int(not neighbors.empty)

    @staticmethod
    def filter_df(df, column, values):
        """
        :param df: DataFrame to filter
        :param column: Column name to filter on
        :param values: Values list to filter by
        :return: The filtered DataFrame
        """

        filtered = None
        if isinstance(values, list):
            filtered = df[df[column].isin(values)]
        elif isinstance(values, str):
            filtered = df[df[column] == values]
        return filtered
