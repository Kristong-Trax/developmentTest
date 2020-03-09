from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.AdjacencyGraph.Builders import AdjacencyGraphBuilder
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
import pandas as pd
import numpy as np
import os
import simplejson
from collections import Counter

from datetime import datetime
from Projects.NESTLEBEVUS.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import
# from KPIUtils_v2.Utils.Consts.DB import
# from KPIUtils_v2.Utils.Consts.PS import
# from KPIUtils_v2.Utils.Consts.GlobalConsts import
# from KPIUtils_v2.Utils.Consts.Messages import
# from KPIUtils_v2.Utils.Consts.Custom import
# from KPIUtils_v2.Utils.Consts.OldDB import

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'krishnat'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'NestleRTD_Template_v1.3.xlsx')
SHELF_POSITION_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                   'Shelf Position Nestle RTD.xlsx')


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


SHEETS = [Consts.KPIS, Consts.SOS, Consts.DISTRIBUTION, Consts.ADJACENCY_BRAND_WITHIN_BAY,
          Consts.ADJACENCY_CATEGORY_WITHIN_BAY, Consts.SHELF_POSITION, Consts.LEAD_ANCHOR_BY_BAY]

SHELF_POSITION_SHEET = [Consts.SHELF_MAP]


class ToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.templates = {}
        self.parse_template()
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.match_scene_item_facts = self.scif.merge(self.match_product_in_scene, how='left',
                                                      on='product_fk')  # Merges scif with mpis on product_fk
        self.block = Block(data_provider)
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result', 'context_id',
                                                'denominator_id', 'denominator_result', 'result', 'score'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

        for shelf_postion_sheet in SHELF_POSITION_SHEET:
            self.templates[shelf_postion_sheet] = pd.read_excel(SHELF_POSITION_PATH, sheet_name=shelf_postion_sheet)

    def main_calculation(self):
        relevant_kpi_template = self.templates[Consts.KPIS]
        # Consts.SOS, Consts.DISTRIBUTION, Consts.ADJACENCY_BRAND_WITHIN_BAY,
        #                                 Consts.ADJACENCY_CATEGORY_WITHIN_BAY, Consts.SHELF_POSITION, Consts.LEAD_ANCHOR_BY_BAY
        foundation_kpi_types = [Consts.SOS, Consts.DISTRIBUTION, Consts.ADJACENCY_BRAND_WITHIN_BAY,
                                        Consts.ADJACENCY_CATEGORY_WITHIN_BAY, Consts.SHELF_POSITION, Consts.LEAD_ANCHOR_BY_BAY]
        foundation_kpi_template = relevant_kpi_template[
            relevant_kpi_template[Consts.KPI_TYPE].isin(foundation_kpi_types)]

        self._calculate_kpis_from_template(foundation_kpi_template)
        self.save_results_to_db()
        return

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df[['result']].fillna(0, inplace=True)
        # self.results_df.fillna(None, inplace=True)
        results = self.results_df.to_dict('records')
        for result in results:
            # try:
            # if result['denominator_result'] == 0 and result['numerator_result']:
            #     del result['denominator_result']
            #     del result['numerator_result']
            result = simplejson.loads(simplejson.dumps(result, ignore_nan=True))
            self.write_to_db(**result)
            # except:
            #     a = 1

    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            calculation_function = self._get_calculation_function_by_kpi_type(row[Consts.KPI_TYPE])
            try:
                kpi_rows = self.templates[row[Consts.KPI_TYPE]][
                    self.templates[row[Consts.KPI_TYPE]][Consts.KPI_NAME].str.encode('utf-8') == row[
                        Consts.KPI_NAME].encode('utf-8')]
            except IndexError:
                pass
            for index, kpi_row in kpi_rows.iterrows():
                result_data = calculation_function(kpi_row)
                if result_data:
                    for result in result_data:
                        # if result['result'] <= 1:
                        #     result['result'] = result['result'] * 100
                        self.results_df.loc[len(self.results_df), result.keys()] = result

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == Consts.SOS:
            return self.calculate_sos
        elif kpi_type == Consts.DISTRIBUTION:
            return self.calculate_distribution
        elif kpi_type == Consts.ADJACENCY_BRAND_WITHIN_BAY:
            return self.calculate_adjacency_brand
        elif kpi_type == Consts.ADJACENCY_CATEGORY_WITHIN_BAY:
            return self.calculate_adjacency_category
        elif kpi_type == Consts.SHELF_POSITION:
            return self.calculate_shelf_position
        elif kpi_type == Consts.LEAD_ANCHOR_BY_BAY:
            return self.calculate_lead_anchor

    def calculate_adjacency_brand(self, row):
        try:
            kpi_name = row[Consts.KPI_NAME]
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            relevant_brands = self._sanitize_values(row[Consts.BRAND_NAME])  # Gets the brand name from the template
            relevant_brand_fk_of_the_relevant_brand = \
                self.all_products.loc[self.all_products.brand_name.isin(relevant_brands), 'brand_fk'].iat[
                    0]  # used to save as the denominator id
            direction = {'UP': 0, 'DOWN': 2, 'RIGHT': 1, 'LEFT': 3}
            result_dict_list = []

            relevant_match_scene_item_facts = self._filter_df(self.match_scene_item_facts, {
                Consts.BRAND_NAME: relevant_brands})  # Filter the merged data frame with the brand to get the relevant dataframe

            if not relevant_match_scene_item_facts.empty:
                for relevant_scene in set(
                        relevant_match_scene_item_facts.scene_id):  # Iterating through the unique scenes existin the merged dataframe
                    score = relevant_scene
                    mcif = relevant_match_scene_item_facts[relevant_match_scene_item_facts.scene_id.isin([relevant_scene])]
                    unique_bay_numbers = set(mcif.bay_number)  # Getting the unique bay numbers in the the scene
                    location = {Consts.SCENE_FK: relevant_scene}
                    for bay in unique_bay_numbers:
                        context_id = bay  # bay number
                        #  Consts.BRAND_FK: relevant_brands,, Consts.BAY_NUMBER:bay
                        # relevant_mpis = self.match_product_in_scene[self.match_product_in_scene.bay_number.isin([bay])]
                        relevant_filters = {Consts.BRAND_NAME: relevant_brands, Consts.BAY_NUMBER: [bay]}
                        # block = self.block.network_x_block_together(relevant_filters,
                        #                                             location=location_filters,
                        #                                             additional={'minimum_facing_for_block': 1,
                        #                                                         'use_masking_only': True})
                        block = self.block.network_x_block_together(relevant_filters,
                                                                    location=location,
                                                                    additional={'allowed_edge_type': ['encapsulated'],
                                                                                'calculate_all_scenes': True,
                                                                                'minimum_facing_for_block': 1,
                                                                                'use_masking_only': True,
                                                                                'allowed_products_filters': {
                                                                                    'product_type': 'Empty'},
                                                                                })
                        passed_block = block[block.is_block == True]
                        if not passed_block.empty:
                            passed_block = passed_block.iloc[passed_block.block_facings.astype(
                                int).idxmax()]  # logic to get the block with the largest number of facings in the block

                            valid_cluster_for_block = passed_block.cluster.nodes.values()
                            relevant_scene_match_fks_for_block = [scene_match_fk for item in valid_cluster_for_block for
                                                                  scene_match_fk in item['scene_match_fk']]

                            '''The below line is used to filter the adjacency graph by the closest edges. Specifically since
                             the edges are determined by masking only, there's a chance that there will be two edges that come
                             out from a single node.'''
                            adj_graph = self.block.adj_graphs_by_scene.values()[0].edge_subgraph(
                                self._filter_redundant_edges(self.block.adj_graphs_by_scene.values()[0]))

                            adj_items = {}  # will contain the scene match fks that are adjacent to the block
                            for match in relevant_scene_match_fks_for_block:
                                for node, node_data in adj_graph.adj[match].items():
                                    if node not in relevant_scene_match_fks_for_block:
                                        product_fk = self.match_scene_item_facts.loc[
                                            self.match_scene_item_facts.scene_match_fk == node, 'product_fk'].iat[0]
                                        if product_fk == 0: # general empty
                                            # we need visibility in to seeing if the product is empty
                                            # since we are saving the brand. If the product is empty, then it will save
                                            # as General. That is why we are saving brand fk 3431 which is brand: empty
                                            important_brand = 3431
                                        elif product_fk == 23211: #irrelevant
                                            # we need visibility in to seeing if the product is irrelevant
                                            # since we are saving the brand. If the product is irrelevant, then it will save
                                            # as General. That is why we are saving brand fk 3432 which is brand: irrelevant
                                            important_brand = 3432
                                        else:
                                            important_brand = self.match_scene_item_facts.loc[
                                            self.match_scene_item_facts.scene_match_fk == node, 'brand_fk'].iat[0]

                                        adj_items[node] = [node_data, important_brand]

                            brand_fks_for_adj_items = np.array([nd[1] for nd in adj_items.values()])
                            node_direction = np.array([nd[0]['direction'] for nd in adj_items.values()])

                            for dir, dir_fk in direction.items():
                                if dir in node_direction:
                                    index_of_revant_brand_fk = np.where(node_direction == dir)
                                    relevant_brand_fks_for_adj_items = brand_fks_for_adj_items[index_of_revant_brand_fk]
                                    values, counts = np.unique(relevant_brand_fks_for_adj_items, return_counts=True)
                                    for j in range(len(values)):
                                        denominator_id = relevant_brand_fk_of_the_relevant_brand  # brand fk of the relevant brand
                                        numerator_id = values[j]  # brand_fk of the adjacency product
                                        numerator_result = dir_fk  # the direction of the adjacency
                                        result = counts[j]
                                        result_dict = {'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                                                       'denominator_id': denominator_id, 'context_id': context_id,
                                                       'numerator_result': numerator_result,
                                                       'result': result, 'score': score}
                                        result_dict_list.append(result_dict)
                return result_dict_list
        except:
            print(self.session_uid)
            print('brand method')
            return []

    def calculate_adjacency_category(self, row):
        try:
            kpi_name = row[Consts.KPI_NAME]
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            relevant_brands = self._sanitize_values(row[Consts.BRAND_NAME])  # Gets the brand name from the template
            relevant_brand_fk_of_the_relevant_brand = \
                self.all_products.loc[self.all_products.brand_name.isin(relevant_brands), 'brand_fk'].iat[
                    0]  # used to save as the denominator id
            direction = {'UP': 0, 'DOWN': 2, 'RIGHT': 1, 'LEFT': 3}
            result_dict_list = []

            relevant_match_scene_item_facts = self._filter_df(self.match_scene_item_facts, {
                Consts.BRAND_NAME: relevant_brands})  # Filter the merged data frame with the brand to get the relevant dataframe

            if not relevant_match_scene_item_facts.empty:
                for relevant_scene in set(
                        relevant_match_scene_item_facts.scene_id):  # Iterating through the unique scenes in the merged dataframe
                    mcif = relevant_match_scene_item_facts[relevant_match_scene_item_facts.scene_id.isin([relevant_scene])]
                    unique_bay_numbers = set(mcif.bay_number)  # Getting the unique bay numbers in the the scene
                    location = {Consts.SCENE_FK: relevant_scene}
                    for bay in unique_bay_numbers:
                        #  Consts.BRAND_FK: relevant_brands,, Consts.BAY_NUMBER:bay
                        # relevant_mpis = self.match_product_in_scene[self.match_product_in_scene.bay_number.isin([bay])]
                        relevant_filters = {Consts.BRAND_NAME: relevant_brands, Consts.BAY_NUMBER: [bay]}
                        # block = self.block.network_x_block_together(relevant_filters,
                        #                                             location=location_filters,
                        #                                             additional={'minimum_facing_for_block': 1,
                        #                                                         'use_masking_only': True})
                        block = self.block.network_x_block_together(relevant_filters,
                                                                    location=location,
                                                                    additional={'allowed_edge_type': ['encapsulated'],
                                                                                'calculate_all_scenes': True,
                                                                                'minimum_facing_for_block': 1,
                                                                                'use_masking_only': True,
                                                                                'allowed_products_filters': {
                                                                                    'product_type': 'Empty'},
                                                                                })
                        passed_block = block[block.is_block == True]
                        if not passed_block.empty:
                            passed_block = passed_block.iloc[passed_block.block_facings.astype(
                                int).idxmax()]  # logic to get the block with the largest number of facings in the block

                            valid_cluster_for_block = passed_block.cluster.nodes.values()
                            relevant_scene_match_fks_for_block = [scene_match_fk for item in valid_cluster_for_block for
                                                                  scene_match_fk in item['scene_match_fk']]

                            '''The below line is used to filter the adjacency graph by the closest edges. Specifically since
                             the edges are determined by masking only, there's a chance that there will be two edges that come
                             out from a single node.'''
                            adj_graph = self.block.adj_graphs_by_scene.values()[0].edge_subgraph(
                                self._filter_redundant_edges(self.block.adj_graphs_by_scene.values()[0]))

                            adj_items = {}  # will contain the scene match fks that are adjacent to the block
                            for match in relevant_scene_match_fks_for_block:
                                for node, node_data in adj_graph.adj[match].items():
                                    if node not in relevant_scene_match_fks_for_block:
                                        product_fk = self.match_scene_item_facts.loc[
                                            self.match_scene_item_facts.scene_match_fk == node, 'product_fk'].iat[0]
                                        if product_fk == 0:  # general empty
                                            # we need visibility in to seeing if the product is empty
                                            # since we are saving the category. If the product is empty, then it will save
                                            # as General. That is why we are saving category fk 26 which is category: empty
                                            important_category = 26
                                        elif product_fk == 23211:  # irrelevant
                                            # we need visibility in to seeing if the product is irrelevant
                                            # since we are saving the category. If the product is irrelevant, then it will save
                                            # as General. That is why we are saving brand fk 27 which is category: irrelevant
                                            important_category = 27
                                        else:
                                            important_category = self.match_scene_item_facts.loc[
                                                self.match_scene_item_facts.scene_match_fk == node, 'category_fk'].iat[0]
                                        adj_items[node] = [node_data, important_category]

                            category_fks_for_adj_items = np.array([nd[1] for nd in adj_items.values()])
                            node_direction = np.array([nd[0]['direction'] for nd in adj_items.values()])

                            for dir, dir_fk in direction.items():
                                if dir in node_direction:
                                    index_of_revant_brand_fk = np.where(node_direction == dir)
                                    relevant_category_fks_for_adj_items = category_fks_for_adj_items[
                                        index_of_revant_brand_fk]
                                    values, counts = np.unique(relevant_category_fks_for_adj_items, return_counts=True)
                                    for j in range(len(values)):
                                        numerator_id = values[j]  # brand_fk of the adjacency product
                                        denominator_id = relevant_brand_fk_of_the_relevant_brand  # brand fk of the relevant brand
                                        numerator_result = dir_fk  # the direction of the adjacency
                                        context_id = bay  # bay number
                                        result = counts[j]  # grouped by category, count
                                        score = relevant_scene
                                        result_dict = {'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                                                       'denominator_id': denominator_id,
                                                       'context_id': context_id,
                                                       'numerator_result': numerator_result,
                                                       'result': result, 'score': score}
                                        result_dict_list.append(result_dict)
                return result_dict_list
        except:
            print(self.session_uid)
            print('category_method')
            return []

    def calculate_distribution(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        relevant_product_fk = self._sanitize_values(row.product_list_pk)

        bool_array_present_products_fk_in_session = pd.np.in1d(relevant_product_fk,
                                                               self.scif.product_fk.unique().tolist())
        present_products_fk_in_session_index = pd.np.flatnonzero(bool_array_present_products_fk_in_session)
        present_products_fk_in_session = pd.np.array(relevant_product_fk).ravel()[present_products_fk_in_session_index]
        absent_products_fk_in_session_index = pd.np.flatnonzero(~ bool_array_present_products_fk_in_session)
        absent_products_fk_in_session = pd.np.array(relevant_product_fk).ravel()[absent_products_fk_in_session_index]

        result_dict_list = []
        for present_product_fk in present_products_fk_in_session:
            result = self.scif[self.scif.product_fk.isin([present_product_fk])].facings.iat[0]
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': present_product_fk,
                           'denominator_id': self.store_id,
                           'result': result}
            result_dict_list.append(result_dict)

        for absent_products_fk_in_session in absent_products_fk_in_session:
            result = 0
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': absent_products_fk_in_session,
                           'denominator_id': self.store_id,
                           'result': result}
            result_dict_list.append(result_dict)
        return result_dict_list

    def calculate_sos(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        iterate_by = self._sanitize_values(row.iterate_by)

        # Have to save the sos by sku. So each sku will have its result (sos) saved
        # The skus relevant are saved in the iterate(in the template)
        sku_relevelant_scif = self.scif[self.scif.product_fk.isin(iterate_by)]

        result_dict_list = []
        for unique_product_fk in set(sku_relevelant_scif.product_fk):
            # The logic for denominator result: The denominator scif is filter by category_fk.
            # The tricky part is the category_fk is determined by the product_fk.
            # So if the category_fk is 1 for product_fk 99. Then the denominator scif is filtered by the
            # category fk 1.
            denominator_relevant_scif = \
                self.scif[self.scif.category_fk.isin(
                    self.scif.category_fk[self.scif.product_fk == unique_product_fk].to_numpy())]
            # denominator_id = self.scif[self.scif]
            denominator_id = denominator_relevant_scif.category_fk.iat[0]
            denominator_result = denominator_relevant_scif[
                row[Consts.OUTPUT]].sum() if not denominator_relevant_scif.empty else 1

            relevant_numerator_scif = self.scif[self.scif.product_fk.isin([unique_product_fk])]
            numerator_result = relevant_numerator_scif[
                row[Consts.OUTPUT]].sum() if not relevant_numerator_scif.empty else 0
            numerator_id = relevant_numerator_scif.product_fk.iat[0]

            result = (float(numerator_result) / denominator_result) * 100
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                           'denominator_id': denominator_id, 'numerator_result': numerator_result,
                           'denominator_result': denominator_result,
                           'result': result}
            result_dict_list.append(result_dict)
        return result_dict_list

    def calculate_shelf_position(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        relevant_product_fks = self._sanitize_values(row[Consts.NUMERATOR_VALUE_1])
        result_dict_list = []

        relevant_mcif = self.match_scene_item_facts[self.match_scene_item_facts.product_fk.isin(relevant_product_fks)]
        if not relevant_mcif.empty:
            shelf_position_dict = {'Bottom':22, 'Middle': 23, 'Eye': 24, 'Top': 25}
            for unique_scene in set(relevant_mcif.scene_id):
                context_id = unique_scene
                relevant_mcif_2 = relevant_mcif[relevant_mcif.scene_id.isin([unique_scene])]
                for bay in set(relevant_mcif_2.bay_number):
                    denominator_id = self._get_shelf_position_id(unique_scene=unique_scene, unique_bay=bay,
                                                                 shelf_position_dict=shelf_position_dict) # return the proper shelf position based on number the number of shelfs using the shelf position dict
                    relevant_mcif_3 = relevant_mcif_2[relevant_mcif_2.bay_number.isin([bay])]
                    group_by_mcif = relevant_mcif_3.groupby(['product_fk', 'shelf_number']).first()
                    relevant_group_by_mcif = group_by_mcif.reset_index('shelf_number')[['shelf_number', 'facings']]
                    for unique_product_fk in set(relevant_group_by_mcif.index):
                        final_mcif_of_product_fk = relevant_group_by_mcif.max()
                        numerator_id = unique_product_fk
                        numerator_result = final_mcif_of_product_fk.facings
                        result = final_mcif_of_product_fk.shelf_number
                        result_dict = {'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                                       'denominator_id': denominator_id, 'context_id': context_id,
                                       'numerator_result': numerator_result,
                                       'result': result}
                        result_dict_list.append(result_dict)
            return result_dict_list

    def calculate_lead_anchor(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        relevant_template_name = row[Consts.TEMPLATE_NAME]
        result_dict_list = []

        relevant_mcif = self.match_scene_item_facts[
            self.match_scene_item_facts.template_name.isin([relevant_template_name])]
        if not relevant_mcif.empty:
            for relevant_scene in set(relevant_mcif.scene_id):
                filtered_mcif = relevant_mcif[relevant_mcif.scene_id.isin([relevant_scene])]
                for bay in set(filtered_mcif.bay_number):
                    bay_filtered_mcif = filtered_mcif[filtered_mcif.bay_number.isin([bay])]
                    product_fks = self._get_the_most_right_or_most_left_product_fks_on_bay(bay_filtered_mcif,
                                                                                           relevant_template_name)
                    unique_product = Counter(product_fks).keys()
                    count_of_product_fks = Counter(product_fks).values()

                    for index in range(len(unique_product)):
                        numerator_id = unique_product[index]  # product fk
                        denominator_id = bay  # bay number
                        context_id = relevant_scene  # scene fk
                        numerator_result = count_of_product_fks[
                            index]  # count of facings of LEAD PRODUCTS  on right or left depending on the scene type
                        denominator_result = sum(count_of_product_fks)  # sum of the total facings of LEAD PRODUCTS
                        result = numerator_result / denominator_result
                        result_dict = {'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                                       'denominator_id': denominator_id, 'context_id': context_id,
                                       'numerator_result': numerator_result,
                                       'result': result}
                        result_dict_list.append(result_dict)

            return result_dict_list

    @staticmethod
    def _sanitize_values(item):
        if pd.isna(item):
            return item
        else:
            items = [x.strip() for x in item.split(',')]
            return items

    @staticmethod
    def _filter_df(df, filters, exclude=0):
        for key, val in filters.items():
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    def _filter_redundant_edges(self, adj_g):
        """Since the edges determines by the masking only, there's a chance that there will be two edges
        that come out from a single node. This method filters the redundant ones (the ones who skip the
        closet adjecent node)"""
        edges_filter = []
        for node_fk, node_data in adj_g.nodes(data=True):
            for direction in ['UP', 'DOWN', 'LEFT', 'RIGHT']:
                edges = [(edge[0], edge[1]) for edge in list(adj_g.edges(node_fk, data=True)) if
                         edge[2]['direction'] == direction]
                if len(edges) <= 1:
                    edges_filter.extend(edges)
                else:
                    edges_filter.append(self._get_shortest_path(adj_g, edges))
        return edges_filter

    def _get_shortest_path(self, adj_g, edges_to_check):
        """ This method gets a list of edge and returns the one with the minimum distance"""
        distance_per_edge = {edge: self._get_edge_distance(adj_g, edge) for edge in edges_to_check}
        shortest_edge = min(distance_per_edge, key=distance_per_edge.get)
        return shortest_edge

    def _get_edge_distance(self, adj_g, edge):
        """
        This method gets an edge and calculate it's length (the distance between it's nodes)
        """
        first_node_coordinate = np.array(self._get_node_display_coordinates(adj_g, edge[0]))
        second_node_coordinate = np.array(self._get_node_display_coordinates(adj_g, edge[1]))
        distance = np.sqrt(np.sum((first_node_coordinate - second_node_coordinate) ** 2))
        return distance

    def _get_node_display_coordinates(self, adj_g, node_fk):
        """
        This method gets a node and extract the Display coordinates (x and y).
        Those attributes were added to each node since this is the attributes we condensed the graph by
        """
        return float(list(adj_g.nodes[node_fk]['rect_x'])[0]), float(list(adj_g.nodes[node_fk]['rect_y'])[0])

    @staticmethod
    def _get_the_most_right_or_most_left_product_fks_on_bay(bay_filtered_mcif, relevant_template_name):
        '''
        :param bay_filtered_mcif: the relevant merge dataframe of mpis and scif
        :param relevant_template_name: the template name (Right door handle or Left door handle)
        :return: If it is a right door handle, gets the product fks of all the right most product of each shelf.
        If it is a left door handle, gets the product fk of all the left most product of each shelf.
        '''

        product_fks = []
        for shelf in set(bay_filtered_mcif.shelf_number):
            shelf_filtered_mcif = bay_filtered_mcif[bay_filtered_mcif.shelf_number.isin([shelf])]

            '''If the template name is 'Right Door Handle', then below loc will get the product fk that
            is the most right product fk of the shelf. If the template name is Left Door Handle, then the 
            below loc will get the most left product fk of the shelf.'''
            most_right_or_left_product_on_shelf = shelf_filtered_mcif.loc[
                shelf_filtered_mcif.rect_x.idxmax(), 'product_fk'] if relevant_template_name == 'Right Door Handle' else \
                shelf_filtered_mcif.loc[shelf_filtered_mcif.rect_x.idxmin(), 'product_fk']
            product_fks.append(most_right_or_left_product_on_shelf)
        return product_fks

    def _get_shelf_position_id(self, unique_scene, unique_bay, shelf_position_dict):
        '''
        :return: Uses the shelf position in the scene and bay and return the id of the shelf
        '''
        shelf_position_relevant_mcif = self.match_scene_item_facts[
            (self.match_scene_item_facts.scene_id.isin([unique_scene])) & (
                self.match_scene_item_facts.bay_number.isin([unique_bay]))].shelf_number.max()
        return shelf_position_dict[self.templates['Shelf Map'].loc[shelf_position_relevant_mcif, unique_bay]]