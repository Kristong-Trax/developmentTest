import os
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency
from Projects.CUBAU.Utils.ParseTemplates import parse_template
from KPIUtils_v2.Calculations.BlockCalculations import Block
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from KPIUtils.DB.Common import Common

BLOCK = 'Block'
ADJACENCY = 'Adjacency'

class Summary:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.adjacency = Adjancency(self.data_provider)
        self.block = Block(self.data_provider)
        self.template_name = 'summary_kpis.xlsx'
        self.TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data', self.template_name)
        self.template_data = parse_template(self.TEMPLATE_PATH, "KPIs")
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.tools = GENERALToolBox(self.data_provider)
        self.common = Common(self.data_provider)
        self.kpi_results_queries = []


    # @log_runtime('Main Calculation')
    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calc_adjacency()
        self.calc_block()
        return

    def calc_adjacency(self):
        # self.adjancency.calculate_adjacency()
        kpi_data = self.template_data.loc[self.template_data['KPI Type'] == ADJACENCY]
        for index, row in kpi_data.iterrows():
            kpi_filters = {}
            kpi_filters['template_display_name'] = row['display name'].split(",")
            score, result, threshold = self.calculate_adjacency(row, kpi_filters)

        return

    def calc_block(self):
        kpi_data = self.template_data.loc[self.template_data['KPI Type'] == BLOCK]
        for index, row in kpi_data.iterrows():
            kpi_filters = {}
            entity_type = row['Group A Entity Type']
            entity_value = row['Group A Entity Value'].split(",")
            kpi_filters[entity_type] = entity_value
            kpi_filters['template_display_name'] = row['display name'].strip().split(",")
            threshold = row['Target']
            block_result = self.block.calculate_block_together(minimum_block_ratio=threshold, **kpi_filters)
            score = 100 if block_result else 0
            self.write_to_db(row, score)
        return

    def write_to_db(self, kpi_data, score):
        kpi_name = kpi_data['KPI']
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name, 2)
        atomic_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name, 3)
        self.common.write_to_db_result(kpi_fk, 2, score)
        self.common.write_to_db_result(atomic_kpi_fk, 3, score)

    # def calculate_adjacency(self, kpi_data, kpi_filters):
    #     score = result = threshold = 0
    #     # params = self.adjacency_data[self.adjacency_data['fixed KPI name'] == kpi]
    #     kpi_filter = kpi_filters.copy()
    #     # target = kpi_data['Target']
    #     # target = float(target.values[0])
    #
    #     entity_type_group_a = kpi_data['Group A Entity Type']
    #     entity_value_group_a = kpi_data['Group A Entity Value'].split(",")
    #     entity_type_group_b = kpi_data['Group B Entity Type']
    #     entity_value_group_b = kpi_data['Group B Entity Value'].split(",")
    #     # kpi_filters['display_name'] = kpi_data['template_display name'].split(",")
    #
    #     group_a = {entity_type_group_a: entity_value_group_a}
    #     group_b = {entity_type_group_b: entity_value_group_b}
    #
    #     allowed_filter = self.tools.get_products_by_filters({'product_type': ['Irrelevant', 'Empty', 'Other']})
    #     allowed_filter_without_other = self.tools.get_products_by_filters({'product_type': ['Irrelevant', 'Empty']})
    #
    #     filters, relevant_scenes = self.tools.separate_location_filters_from_product_filters(**kpi_filter)
    #
    #     for scene in relevant_scenes:
    #         adjacency = self.adjacency.calculate_adjacency(group_a, group_b, {'scene_fk': scene}, allowed_filter,
    #                                                        allowed_filter_without_other, a_target=0.65, b_target=0.65, target=0.65)
    #         score = result = adjacency
            # if adjacency:
            #     direction = params.get('Direction', 'All').values[0]
            #     if direction == 'All':
            #         score = result = adjacency
            #     else:
            #         # a = self.data_provider.products[self.tools.get_filter_condition(self.data_provider.products, **group_a)]['product_fk'].tolist()
            #         # b = self.data_provider.products[self.tools.get_filter_condition(self.data_provider.products, **group_b)]['product_fk'].tolist()
            #         # a = self.scif[self.scif['product_fk'].isin(a)]['product_name'].drop_duplicates()
            #         # b = self.scif[self.scif['product_fk'].isin(b)]['product_name'].drop_duplicates()
            #
            #         edges_a = self.block.calculate_block_edges(minimum_block_ratio=a_target, **dict(group_a, **{'scene_fk': scene}))
            #         edges_b = self.block.calculate_block_edges(minimum_block_ratio=b_target, **dict(group_b, **{'scene_fk': scene}))
            #
            #         if edges_a and edges_b:
            #             if direction == 'Vertical':
            #                 if sorted(set(edges_a['shelfs'])) == sorted(set(edges_b['shelfs'])) and \
            #                         len(set(edges_a['shelfs'])) == 1:
            #                     score = result = 0
            #                 elif max(edges_a['shelfs']) <= min(edges_b['shelfs']):
            #                     score = 100
            #                     result = 1
            #                 elif max(edges_b['shelfs']) <= min(edges_a['shelfs']):
            #                     score = 100
            #                     result = 1
            #             elif direction == 'Horizontal':
            #                 if set(edges_a['shelfs']).intersection(edges_b['shelfs']):
            #                     extra_margin_a = (edges_a['visual']['right'] - edges_a['visual']['left']) / 10
            #                     extra_margin_b = (edges_b['visual']['right'] - edges_b['visual']['left']) / 10
            #                     edges_a_right = edges_a['visual']['right'] - extra_margin_a
            #                     edges_b_left = edges_b['visual']['left'] + extra_margin_b
            #                     edges_b_right = edges_b['visual']['right'] - extra_margin_b
            #                     edges_a_left = edges_a['visual']['left'] + extra_margin_a
            #                     if edges_a_right <= edges_b_left:
            #                         score = 100
            #                         result = 1
            #                     elif edges_b_right <= edges_a_left:
            #                             score = 100
            #                             result = 1
        # return score, result, threshold

