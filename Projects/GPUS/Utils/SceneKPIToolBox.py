
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph

# from Trax.Utils.Logging.Logger import Log
import pandas as pd
from collections import defaultdict
import os

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.GPUS.Utils.Const import Const

__author__ = 'sam, nicolaske'


class SceneGPUSToolBox:

    def __init__(self, data_provider, common, output):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.scene = self.scene_info.loc[0, 'scene_fk']
        self.templates = self.data_provider[Data.TEMPLATES]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.mpis = self.make_mpis()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif[~(self.scif['product_type'] == 'Irrelevant')]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER].iloc[0, 1])
        self.gp_manufacturer = self.get_gp_manufacturer()
        self.gp_categories = self.get_gp_categories()
        self.gp_brands = self.get_gp_brands()
        self.man_fk_filter = {'manufacturer_name': list(self.gp_manufacturer.keys())}
        self.cat_filter = {'category': list(self.gp_categories.keys())}
        self.brand_filter = {'brand_name': list(self.gp_brands.keys())}
        self.kpi_results = []
        self.dedupe = set()


    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        if not self.filter_df(self.mpis, self.brand_filter).empty:
            self.calculate_adjacency('Adjacency', list(Const.HIERARCHY.keys()))
        for result in self.kpi_results:
            self.write_to_db(**result)
        return

    def base_adj_graph(self, additional_attributes=None):
        mpis_filter = {'scene_fk': self.scene}
        mpis = self.filter_df(self.mpis, mpis_filter)
        mpis = mpis[(mpis['product_type'] != Const.IRRELEVANT) & (mpis['category'] != Const.GENERAL) &
                    (mpis['brand_name'] != Const.GENERAL)]
        all_graph = AdjacencyGraph(mpis, None, self.products, name=None, adjacency_overlap_ratio=.4)
        return mpis, all_graph, mpis_filter

    def base_adjacency(self):
        data = defaultdict(dict)
        mpis, all_graph, filters = self.base_adj_graph()
        brands = set(self.filter_df(self.mpis, self.brand_filter)['brand_name'])
        for brand in brands:
            for edge_dir in Const.ALLOWED_EDGES:
                items = set(self.filter_df(self.mpis, {'brand_name': brand})['scene_match_fk'])
                g = self.prune_edges(all_graph.base_adjacency_graph.copy(), [edge_dir])

                match_to_node = {int(node['match_fk']): i for i, node in g.nodes(data=True)}
                node_to_match = {val: key for key, val in match_to_node.items()}
                edge_matches = set(sum([[node_to_match[i] for i in g[match_to_node[item]].keys()]
                                        for item in items], []))
                adjacent_items = edge_matches - items
                adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_items))]
                adj_mpis['num_fk'] = self.gp_brands[brand]
                data[brand][edge_dir] = adj_mpis
        return data

    def calculate_adjacency(self, base_kpi_name, levels):
        data = self.base_adjacency()
        for brand, adj_mpises in data.items():
            for edge_dir, adj_mpis in adj_mpises.items():
                for level in levels:
                    base_level = level.split('_')[0]
                    real_edge = Const.ALLOWED_EDGES - {edge_dir}
                    adj_mpis['kpi'] = '{} {} {}'.format(real_edge.pop().capitalize(), base_level.capitalize(),
                                                        base_kpi_name)
                    self.adj_mpis_to_result(adj_mpis, edge_dir, 'kpi', 'num_fk', '{}_fk'.format(level),
                                            self_id=Const.HIERARCHY[level]['ident'],
                                            parent=Const.HIERARCHY[level]['parent'])

    def adj_mpis_to_result(self, df, dir, kpi_col, num_id_col, den_id_col, self_id=None, parent=None):
        for i, row in df.iterrows():
            res_ident = '{}_{}'.format(dir, row[self_id]) if self_id else None
            res_parent = '{}_{}'.format(dir, row[parent]) if parent else None
            dupe_tuple = (row[kpi_col], row[num_id_col], row[den_id_col], res_ident, res_parent)
            if dupe_tuple not in self.dedupe:
                self.dedupe.add(dupe_tuple)
                kpi_res = {'kpi_name': row[kpi_col],
                           'numerator_id': row[num_id_col],
                           'denominator_id': row[den_id_col],
                           'score': 1,
                           'result': 1,
                           'ident_result': res_ident,
                           'ident_parent': res_parent}
                self.kpi_results.append(kpi_res)

    def prune_edges(self, g, allowed_edges, keep_or_cut=Const.KEEP):
        for node in g.nodes():
            for edge in g.edges(node, data=True):
                edge_dir = edge[2]['direction']
                if keep_or_cut == Const.KEEP:
                    if edge_dir not in allowed_edges:
                        g.remove_edge(node, edge[1])
                else:
                    if edge_dir in allowed_edges:
                        g.remove_edge(node, edge[1])
        return g

    @staticmethod
    def filter_df(df, filters, exclude=0):
        cols = set(df.columns)
        for key, val in filters.items():
            if key not in cols:
                return pd.DataFrame()
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    def get_gp_categories(self):
        cats = self.products[self.products['manufacturer_fk'] == self.manufacturer_fk][['category', 'category_fk']]\
                              .drop_duplicates().set_index('category')['category_fk'].to_dict()
        return cats

    def get_gp_brands(self):
        brands = self.products[self.products['manufacturer_fk'] == self.manufacturer_fk][['brand_name', 'brand_fk']]\
                              .drop_duplicates().set_index('brand_name')['brand_fk'].to_dict()
        return brands

    def get_gp_manufacturer(self):
        name = self.products[self.products['manufacturer_fk'] == self.manufacturer_fk].reset_index()
        name = '' if name.empty else name.loc[0, 'manufacturer_name']
        return {name: self.manufacturer_fk}

    def make_mpis(self):
        mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.templates, on='template_fk', suffixes=['', '_t'])
        mpis = mpis[mpis['stacking_layer'] == 1]
        return mpis

    def write_to_db(self, kpi_name=None, score=0, result=None, target=None, numerator_result=None,
                    denominator_result=None,
                    numerator_id=999, denominator_id=999, ident_result=None, ident_parent=None, kpi_fk=None):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        if not kpi_fk:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        self.common.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=target,
                                       numerator_result=numerator_result, denominator_result=denominator_result,
                                       numerator_id=numerator_id, denominator_id=denominator_id,
                                       identifier_result=ident_result, identifier_parent=ident_parent, by_scene=True)


