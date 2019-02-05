
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
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.mpis = self.make_mpis()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER].iloc[0, 1])
        self.manufacturer = self.get_gp_name()
        self.gp_categories = self.get_gp_categories()
        self.gp_brands = self.get_gp_brands()

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # self.calculate_adjacency()

        score = 0
        return score

    # def shareofshelf(self):
    #     def calculate_main_kpi(self, main_line):
    #         kpi_name = main_line[Const.KPI_NAME]
    #         kpi_type = main_line[Const.Type]
    #         template_groups = self.does_exist(main_line, Const.TEMPLATE_GROUP)
    #
    #         general_filters = {}
    #
    #         scif_template_groups = self.scif['template_group'].unique().tolist()
    #         # encoding_fixed_list = [template_group.replace("\u2013","-") for template_group in scif_template_groups]
    #         # scif_template_groups = encoding_fixed_list
    #
    #         store_type = self.store_info["store_type"].iloc[0]
    #         store_types = self.does_exist_store(main_line, Const.STORE_TYPES)
    #         if store_type in store_types:
    #
    #             if template_groups:
    #                 if ('All' in template_groups) or bool(set(scif_template_groups) & set(template_groups)):
    #                     if not ('All' in template_groups):
    #                         general_filters['template_group'] = template_groups
    #                     if kpi_type == Const.SOVI:
    #                         relevant_template = self.templates[kpi_type]
    #                         relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
    #
    #                         if relevant_template["numerator param 1"].all() and relevant_template[
    #                             "denominator param"].all():
    #                             function = self.get_kpi_function(kpi_type)
    #                             for i, kpi_line in relevant_template.iterrows():
    #                                 result, score = function(kpi_line, general_filters)
    #                     else:
    #                         pass
    #
    #             else:
    #
    #                 pass

    def base_adj_graph(self, scene, general_filters, use_allowed=0, additional_attributes=None):
        product_attributes = ['rect_x', 'rect_y']
        if additional_attributes is not None:
            product_attributes = product_attributes + additional_attributes
        mpis_filter = {'scene_fk': scene}
        mpis = self.filter_df(self.mpis, mpis_filter)
        items = set(mpis['scene_match_fk'].values)
        all_graph = AdjacencyGraph(mpis, None, self.products,
                                   product_attributes=product_attributes + list(filters.keys()),
                                   name=None, adjacency_overlap_ratio=.4)
        return items, mpis, all_graph, mpis_filter

    def base_adjacency(self, kpi_name, kpi_line, relevant_scif, general_filters, limit_potential=1, use_allowed=1,
                       col_list=Const.REF_COLS):
        allowed_edges = ['left', 'right']
        all_results = {}
        scene = self.scene
        items, mpis, all_graph, filters = self.base_adj_graph(scene, kpi_line, general_filters,
                                                              use_allowed=use_allowed, gmi_only=0)

        for edge_dir in allowed_edges:
            g = self.prune_edges(all_graph.base_adjacency_graph.copy(), [edge_dir])

            match_to_node = {int(node['match_fk']): i for i, node in g.nodes(data=True)}
            node_to_match = {val: key for key, val in match_to_node.items()}
            edge_matches = set(sum([[node_to_match[i] for i in g[match_to_node[item]].keys()]
                                    for item in items], []))
            adjacent_items = edge_matches - items
            adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_items)) &
                            (~mpis['product_type'].isin(Const.SOS_EXCLUDE_FILTERS))]
            adjacent_sections = set(sum([list(adj_mpis[col].unique()) for col in col_list], []))
            if limit_potential:
                potential_results = set(self.get_results_value(kpi_line))
                adjacent_sections = list(adjacent_sections & potential_results)
            all_results[edge_dir] = [adjacent_sections, len(adjacent_items) / float(len(items))]
        return all_results

    def calculate_adjacency(self, kpi_name, kpi_line, relevant_scif, general_filters):
        all_results = self.base_adjacency(kpi_name, kpi_line, relevant_scif, general_filters, col_list=res_col)
        ret_values = []
        for result in sum([x for x, y in all_results.values()], []):
            if not result and kpi_line[Const.TYPE] == Const.ANCHOR_LIST:
                result = Const.END_OF_CAT
            result_fk = self.result_values_dict[result]
            ret_values.append({'denominator_id': result_fk, 'score': 1, 'result': result_fk, 'target': 0})
        return ret_values

    def get_gp_categories(self):
        cats = self.products.set_index('manufacturer_fk').loc[self.manufacturer_fk, ['category', 'category_fk']]\
                              .drop_duplicates().reset_index(drop=True)
        return cats

    def get_gp_brands(self):
        brands = self.products.set_index('manufacturer_fk').loc[self.manufacturer_fk, ['brand_name', 'brand_fk']]\
                              .drop_duplicates().reset_index(drop=True)
        return brands

    def get_gp_name(self):
        name = self.products.set_index('manufacturer_fk').loc[self.manufacturer_fk, 'manufacturer_name'].iloc[0]
        return name

    def make_mpis(self):
        mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.templates, on='template_fk', suffixes=['', '_t'])
        return mpis