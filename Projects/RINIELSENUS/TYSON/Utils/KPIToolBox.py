import pandas as pd
import numpy as np
import itertools

from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Trax.Algo.Calculations.Core.DataProvider import Data

import Projects.RINIELSENUS.TYSON.Utils.Const as Const
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block

__author__ = 'Trevaris'

COLUMNS = ['product_fk', 'product_name', 'bay_number', 'scene_fk', 'brand_fk', 'brand_name',
           'manufacturer_fk', 'manufacturer_name', 'category_fk', 'category']


class TysonToolBox:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.common = Common(data_provider)
        self.output = output
        self.ps_data_provider = PsDataProvider(data_provider, output)
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.manufacturer_id = self.get_manufacturer_id_from_manufacturer_name(Const.MANUFACTURER)
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.template_info = self.data_provider[Data.TEMPLATES]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.block = Block(self.data_provider, self.output, common=self.common)

        self.mpis = self.match_product_in_scene \
            .merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s'])[COLUMNS]

    def main_calculation(self):
        self.calculate_shelf_neighbors('scrambles', 'breakfast meat')
        self.calculate_shelf_neighbors('scrambles', 'irrelevant')
        self.calculate_shelf_neighbors('ore ida', 'breakfast meat')
        self.calculate_shelf_neighbors('ore ida', 'irrelevant')
        self.calculate_max_block_adjacency('scrambles', 'ore ida')
        self.calculate_adjacent_bay('breakfast meat', 'irrelevant')

    def calculate_shelf_neighbors(self, target, neighbor):
        """
        Determines whether any products in `target` are located in the same scene and bay as any products in `neighbor`.

        :param target: Key referencing the target product.
        :param neighbor: Key referencing the products being compared.
        """

        kpi = Const.KPIs[(target, neighbor)]
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi)
        brand_id = self.get_brand_id_from_brand_name(Const.BRANDs.get(target))

        result = self.neighbors(target, neighbor, neighbor_type='product') if neighbor == 'irrelevant' \
            else self.neighbors(target, neighbor)

        self.common.write_to_db_result(
            fk=kpi_id,
            numerator_id=brand_id,
            numerator_result=result,
            denominator_id=self.store_id,
            denominator_result=1,
            result=result
        )

    def calculate_max_block_adjacency(self, target, neighbor):
        """
        Calculates the Max Block Adjacency for `target` and `neighbor` products.

        :param target: Key for customer products.
        :param neighbor: Key for competitor products.
        """

        kpi = Const.KPIs[('scrambles', 'ore ida')]
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi)
        target_max_block, target_scene = self.get_max_block_from_products(Const.PRODUCTS[target])
        neighbor_max_block, neighbor_scene = self.get_max_block_from_products(Const.PRODUCTS[neighbor])

        result = 0
        if target_max_block and neighbor_max_block and (target_scene == neighbor_scene):
            possible_adjacencies = itertools.product(target_max_block.nodes, neighbor_max_block.nodes)
            adj_graph = self.block.adj_graphs_by_scene
            directed_edges = [list(adj_graph[key].edges) for key in adj_graph.keys() if int(key[0]) == target_scene][0]
            complimentary_edges = [edge[::-1] for edge in directed_edges if edge[::-1] not in directed_edges]
            all_edges = directed_edges + complimentary_edges
            result = int(any(True for edge in possible_adjacencies if edge in all_edges))

        self.common.write_to_db_result(
            fk=kpi_id,
            numerator_id=self.manufacturer_id,
            numerator_result=result,
            denominator_id=self.store_id,
            denominator_result=1,
            result=result
        )

    def calculate_adjacent_bay(self, target, neighbor):
        """
        Determines whether any products in `target` are located in the same scene
        and same or adjacent bay as any products in `neighbor`.

        :param target: Key referencing the target product.
        :param neighbor: Key referencing the products being compared.
        """
        kpi = Const.KPIs.get((target, neighbor))
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi)
        result = self.neighbors(target, neighbor,
                                target_type='category', neighbor_type='product', same_bay=False)

        self.common.write_to_db_result(
            fk=kpi_id,
            numerator_id=self.manufacturer_id,
            numerator_result=result,
            denominator_id=self.store_id,
            denominator_result=1,
            result=result
        )

    def neighbors(self, target, neighbor, target_type='product', neighbor_type='category', same_bay=True):
        """
        Determine whether any of the products in `target` and `neighbor` are
            in the same scene and same bay (if `same_bay` is True)
        or  in the same scene and same or adjacent bay (if `same_bay`is False).

        :param target:
        :param neighbor:
        :param target_type:
        :param neighbor_type:
        :param same_bay: Indicates whether to count the same bay or also adjacent bays.
        :return: Returns 1 if any products are neighbors, else 0.
        """

        target_ids = None
        neighbor_ids = None

        if target_type == 'product':
            target_ids = [self.get_product_id_from_product_name(product) for product in Const.PRODUCTS[target]]
        elif target_type == 'category':
            target_ids = [self.get_category_id_from_category_name(Const.CATEGORIES[target])]

        if neighbor_type == 'category':
            neighbor_ids = [self.get_category_id_from_category_name(category) for category in Const.CATEGORIES.values()]
        elif neighbor_type == 'product':
            neighbor_ids = [self.get_product_id_from_product_name(product) for product in Const.PRODUCTS[neighbor]]

        target_products = self.filter_df(self.mpis, target_type+'_fk', target_ids).drop_duplicates()
        neighbor_products = self.filter_df(self.mpis, neighbor_type+'_fk', neighbor_ids).drop_duplicates()

        if same_bay:
            neighbors = target_products.merge(neighbor_products, how='inner', on=['scene_fk', 'bay_number'])
        else:
            scenes = pd.concat([target_products['scene_fk'], neighbor_products['scene_fk']]).unique()
            probe_groups = self.get_probe_groups(scenes)
            target_products = target_products.merge(probe_groups, on=['scene_fk', 'product_fk'])
            neighbor_products = neighbor_products.merge(probe_groups, on=['scene_fk', 'product_fk'])
            scene_neighbors = target_products.merge(neighbor_products, on=['scene_fk'], suffixes=['', '_y'])
            neighbors = scene_neighbors.apply(
                lambda row: 1 if abs(int(row['bay_number']) - int(row['bay_number_y'])) < 2
                and row['group_id'] == row['group_id_y'] else np.nan,
                axis='columns'
            ).dropna()

        return int(not neighbors.empty)

    def get_brand_id_from_brand_name(self, brand_name):
        # return self.all_products.set_index(['brand_name']).loc[brand_name, 'brand_fk'].iloc[0]
        # return self.all_products.loc[self.all_products['brand_name'] == brand_name, 'brand_fk'].values[0]
        return self.all_products[self.all_products['brand_name'] == brand_name].iloc[0].at['brand_fk']

    def get_product_id_from_product_name(self, product_name):
        return self.all_products.set_index(['product_english_name']).loc[product_name, 'product_fk']

    def get_category_id_from_category_name(self, category_name):
        return self.all_products.set_index(['category']).loc[category_name, 'category_fk'].iloc[0]

    def get_manufacturer_id_from_manufacturer_name(self, manufacturer_name):
        return self.all_products.loc[self.all_products['manufacturer_name'] == manufacturer_name,
                                     'manufacturer_fk'].values[0]

    @staticmethod
    def filter_df(df, column, values):
        """
        Returns a subset of `df` whose values are in `values`.

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

    def get_max_block_from_products(self, products):
        """
        Get max block based on list of product names.

        :param products: List of product names.
        :return: (Max block graph, scene_fk).
        """

        product_ids = [self.get_product_id_from_product_name(product) for product in products]
        blocks = self.block.network_x_block_together(
            {'product_fk': product_ids},
            additional={
                'calculate_all_scenes': True,
                'use_masking_only': True,
                'minimum_facing_for_block': 2
            }
        )
        blocks.sort_values(by=['block_facings', 'facing_percentage'], ascending=False, inplace=True)

        max_block = scene_id = None
        if not blocks.empty and any(blocks['is_block']):
            max_block = blocks['cluster'].iloc[0]
            scene_id = blocks['scene_fk'].iloc[0]

        return max_block, scene_id

    def get_probe_groups(self, scenes):
        """
        Retrieves from probedata.stitching_probe_info the probe groups for all products in `scenes`.

        :param scenes: np.ndarray of scene_fk.
        :return: dataframe containing product_fk, scene_fk, and group_id of products in `scenes`.
        """

        query = """
            SELECT DISTINCT mpip.product_fk, spi.group_id, ssi.scene_fk 
                FROM probedata.stitching_probe_info AS spi
                LEFT JOIN probedata.stitching_scene_info AS ssi ON ssi.pk = spi.stitching_scene_info_fk
                LEFT JOIN probedata.match_product_in_probe AS mpip ON mpip.probe_fk = spi.probe_fk
                WHERE ssi.scene_fk IN ({});
        """.format(", ".join([str(scene) for scene in scenes]))
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db)
