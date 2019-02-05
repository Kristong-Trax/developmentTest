
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph

# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.GPUS.Utils.Const import Const

__author__ = 'nicolaske'


class GPUSToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.mpis = self.make_mpis()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER].iloc[0, 1])
        self.gp_manufacturer = self.get_gp_manufacturer()
        self.gp_categories = self.get_gp_categories()
        self.gp_brands = self.get_gp_brands()
        self.man_fk_filter = {'manufacturer_fk': [self.manufacturer_fk]}
        self.cat_filter = {'category': list(self.gp_categories.keys())}
        self.brand_filter = {'brand_name': list(self.gp_brands.keys())}
        self.kpi_results = []


    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        if not self.filter_df(self.scif, self.brand_filter).empty:
            # self.calculate_adjacency(kpi)
            kpi='asd'
            self.calculate_manufacturer_facings_sos(kpi)

        if not self.filter_df(self.scif, self.brand_filter).empty:
            pass
        score = 0
        return score

    def calculate_manufacturer_facings_sos(self, kpi):

        results = self.calculate_sos(kpi, nums=self.man_fk_filter, dens=self.cat_filter,
                                     num_fks=self.gp_manufacturer, den_fks=self.gp_categories,
                                     sum_col=Const.FACINGS, calc_func=self.facings_sos_calc)


    def calculate_brand_facings_sos(self, kpi):
        pass
        # num_scif, den_scif = self.make_num_and_den_scifs(num_filters, den_filters)

    def calculate_sos(self, kpi, nums, dens, num_fks, den_fks, sum_col, calc_func):
        den_groups = self.filter_df(self.scif, dens).groupby(dens.keys()[0])
        for i, den_df in den_groups:
            num_groups = den_df.groupby(nums.keys()[0])
            for j, num_df in num_groups:
                if num_df.empty:
                    continue
                num = num_df[sum_col].sum()
                den = den_df[sum_col].sum()
                res = (float(num) / den) * 100 if num else 0

                self.kpi_results.append(
                    

                                        )




    def base_adj_graph(self, scene, additional_attributes=None):
        product_attributes = ['rect_x', 'rect_y']
        if additional_attributes is not None:
            product_attributes = product_attributes + additional_attributes
        mpis_filter = {'scene_fk': scene}
        mpis_filter.update(self.cat_filter)
        mpis = self.filter_df(self.mpis, mpis_filter)
        all_graph = AdjacencyGraph(mpis, None, self.products,
                                   product_attributes=product_attributes,
                                   name=None, adjacency_overlap_ratio=.4)
        return mpis, all_graph, mpis_filter

    def base_adjacency(self):
        allowed_edges = ['left', 'right']
        all_results = {}
        scene = self.scene
        mpis, all_graph, filters = self.base_adj_graph(scene)
        items = self.filter_df(self.mpis, self.brand_filter)

        for edge_dir in allowed_edges:
            g = self.prune_edges(all_graph.base_adjacency_graph.copy(), [edge_dir])

            match_to_node = {int(node['match_fk']): i for i, node in g.nodes(data=True)}
            node_to_match = {val: key for key, val in match_to_node.items()}
            edge_matches = set(sum([[node_to_match[i] for i in g[match_to_node[item]].keys()]
                                    for item in items], []))
            adjacent_items = edge_matches - items
            adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_items)) &
                            (~mpis['product_type'].isin(Const.SOS_EXCLUDE_FILTERS))]
        #     adjacent_sections = set(sum([list(adj_mpis[col].unique()) for col in col_list], []))
        #     all_results[edge_dir] = [adjacent_sections, len(adjacent_items) / float(len(items))]
        # return all_results

    def calculate_adjacency(self):
        all_results = self.base_adjacency()
        ret_values = []
        for result in sum([x for x, y in all_results.values()], []):
            # if not result:
            #     result = Const.END_OF_CAT
            result_fk = self.result_values_dict[result]
            ret_values.append({'denominator_id': result_fk, 'score': 1, 'result': result_fk, 'target': 0})
        return ret_values

    def prune_edges(self, g, allowed_edges, keep_or_cut='keep'):
        for node in g.nodes():
            for edge_id, edge in g[node].items():
                for edge_dir in edge.values():
                    if keep_or_cut == 'keep':
                        if edge_dir not in allowed_edges:
                            del g[node][edge_id]
                    else:
                        if edge_dir in allowed_edges:
                            del g[node][edge_id]
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
        cats = self.products.set_index('manufacturer_fk').loc[self.manufacturer_fk, ['category', 'category_fk']]\
                              .drop_duplicates().set_index('category').to_dict('index')
        return cats

    def get_gp_brands(self):
        brands = self.products.set_index('manufacturer_fk').loc[self.manufacturer_fk, ['brand_name', 'brand_fk']]\
                              .drop_duplicates().set_index('brand_name').to_dict('index')
        return brands

    def get_gp_manufacturer(self):
        name = self.products.set_index('manufacturer_fk').loc[self.manufacturer_fk, 'manufacturer_name'].iloc[0]
        return {name: self.manufacturer_fk}

    def make_mpis(self):
        mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.templates, on='template_fk', suffixes=['', '_t'])
        return mpis

    def write_to_db(self, kpi_name, score=0, result=None, target=None, numerator_result=None,
                    denominator_result=None, numerator_id=999, denominator_id=999):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        self.common.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=target,
                                       numerator_result=numerator_result, denominator_result=denominator_result,
                                       numerator_id=numerator_id, denominator_id=denominator_id)

