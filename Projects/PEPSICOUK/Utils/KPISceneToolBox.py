
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np

from Projects.PEPSICOUK.Utils.CommonToolBox import PEPSICOUKCommonToolBox
import os

# from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency
from KPIUtils_v2.Calculations.AdjacencyCalculations import Block

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

__author__ = 'natalyak'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class PEPSICOUKSceneToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    NUMBER_OF_FACINGS = 'Number of Facings'
    TOTAL_LINEAR_SPACE = 'Total Linear Space'
    NUMBER_OF_BAYS = 'Number of bays'
    NUMBER_OF_SHELVES = 'Number of shelves'
    PEPSICO = 'PEPSICO'
    PLACEMENT_BY_SHELF_NUMBERS_TOP = 'Placement by shelf numbers_Top'
    SHELF_PLACEMENT = 'Shelf Placement'
    SHELF_PLACEMENT_VERTICAL_LEFT = 'Shelf Placement Vertical Left'
    SHELF_PLACEMENT_VERTICAL_CENTER = 'Shelf Placement Vertical Center'
    SHELF_PLACEMENT_VERTICAL_RIGHT = 'Shelf Placement Vertical Right'
    PRODUCT_BLOCKING = 'Product Blocking'
    PRODUCT_BLOCKING_ADJACENCY = 'Product Blocking Adjacency'
    PRIMARRY_SHELF = 'Primary Shelf'
    
    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        # self.common = common
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]  # initial scif - check if it works for scene calculations

        self.toolbox = GENERALToolBox(data_provider)
        self.commontools = PEPSICOUKCommonToolBox(data_provider)

        self.custom_entities = self.commontools.custom_entities
        self.on_display_products = self.commontools.on_display_products
        self.exclusion_template = self.commontools.exclusion_template
        self.filtered_scif = self.commontools.filtered_scif
        self.filtered_matches = self.commontools.filtered_matches
        self.excluded_matches = self.compare_matches()
        # self.filtered_scif = self.scif  # filtered scif acording to exclusion template
        # self.filtered_matches = self.match_product_in_scene  # filtered scif according to exclusion template
        # self.set_filtered_scif_and_matches_for_all_kpis(self.scif,
        #                                                 self.match_product_in_scene)  # also sets scif and matches in data provider
        self.scene_bay_shelf_product = self.commontools.scene_bay_shelf_product
        self.external_targets = self.commontools.external_targets
        self.own_manuf_fk = self.all_products[self.all_products['manufacturer_name'] == self.PEPSICO]['manufacturer_fk'].values[0]
        self.block = Block(self.data_provider)
        self.adjacency = Adjancency(self.data_provider)
        self.block_results = pd.DataFrame(columns=['Group Name', 'Score'])

    def compare_matches(self):
        initial_matches = set(self.match_product_in_scene['probe_match_fk'].values.tolist())
        filtered_matches = set(self.filtered_matches['probe_match_fk'].values.tolist())
        excluded_matches = filtered_matches.difference(initial_matches)
        return excluded_matches

    def main_function(self):
        # if not self.filtered_matches.empty:
        if self.scif['location_type'].values[0] == self.PRIMARRY_SHELF:
            self.calculate_external_kpis()
            self.calculate_internal_kpis()

    def calculate_external_kpis(self):
        self.calculate_product_blocking()
        self.calculate_adjacency()

    def calculate_internal_kpis(self):
        self.calculate_number_of_facings_and_linear_space()
        self.calculate_number_of_bays_and_shelves()
        self.calculate_shelf_placement_horizontal()
        self.calculate_shelf_placement_vertical()

    def calculate_number_of_facings_and_linear_space(self):
        facing_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.NUMBER_OF_FACINGS)
        linear_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.TOTAL_LINEAR_SPACE)
        for i, row in self.filtered_scif:
            self.common.write_to_db_result(fk=facing_kpi_fk, numerator_id=row['product_fk'], result=row['updated_facings'],
                                           denominator_id=self.store_id, by_scene=True)
            self.common.write_to_db_result(fk=linear_kpi_fk, numerator_id=row['product_fk'], denominator_id=self.store_id,
                                           result=row['updated_gross_length'], by_scene=True)

    def calculate_number_of_bays_and_shelves(self):
        bays_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.NUMBER_OF_BAYS)
        shelves_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.NUMBER_OF_SHELVES)
        matches = self.match_product_in_scene[~(self.match_product_in_scene['bay_number'] == -1)] # is it filtered or regular? # bay -1?

        bays_in_scene = matches['bay_number'].unique().tolist()
        bays_num = len(bays_in_scene)
        bay_shelf = matches.drop_duplicates(subset=['bay_number', 'shelf_number'])
        shelf_num = len(bay_shelf)
        # shelf_num = 0
        # for bay in bays_in_scene:
        #     bay_matches = self.filtered_matches[self.filtered_matches['bay_number'] == bay]
        #     shelf_num += len(bay_matches['shelf_number'].unique().tolist()) # is it filtered or regular?
        self.common.write_to_db_result(fk=bays_kpi_fk, numerator_id=self.PEPSICO, result=bays_num,
                                       denominator_id=self.store_id, by_scene=True)
        self.common.write_to_db_result(fk=shelves_kpi_fk, numerator_id=self.PEPSICO, result=shelf_num,
                                       denominator_id=self.store_id, by_scene=True)

    def calculate_shelf_placement_horizontal(self):
        # shelf_placement_targets = self.commontools.get_shelf_placement_kpi_targets_data()
        external_targets = self.commontools.all_targets_unpacked
        shelf_placmnt_targets = external_targets[external_targets['operation_type'] == self.SHELF_PLACEMENT]
        if not shelf_placmnt_targets.empty:
            bay_max_shelves = self.get_scene_bay_max_shelves(shelf_placmnt_targets)
            bay_all_shelves = bay_max_shelves.drop_duplicates(subset=['bay_number', 'shelves_all_placements'],
                                                              keep='first')
            relevant_matches = self.filter_out_irrelevant_matches(bay_all_shelves)
            for i, row in bay_max_shelves:
                shelf_list = map(lambda x: float(x), row['Shelves From Bottom To Include (data)'].split(','))
                relevant_matches.loc[(relevant_matches['bay_number'] == row['bay_number']) &
                                     (relevant_matches['shelf_number_from_bottom'].isin(shelf_list)), 'position'] = row['type']
            kpi_results = self.get_kpi_results_df(relevant_matches, bay_max_shelves)
            for i, result in kpi_results.iterrows():
                self.common.write_to_db_result(fk=result['kpi_level_2_fk'], numerator_id=result['product_fk'],
                                               denominator_id=result['product_fk'],
                                               denominator_result=result['total_facings'],
                                               numerator_result=result['count'], result=result['ratio'],
                                               score=result['ratio'])
            # maybe add summarizing parent - commented out

    def calculate_shelf_placement_vertical(self):
        left_edge = self.match_product_in_scene['rect_x'].min()
        right_edge = self.match_product_in_scene['rect_x'].max()
        shelf_length = float(right_edge - left_edge)
        matches = self.match_product_in_scene.copy()
        shift = 0 - left_edge
        matches['adjusted_rect_x'] = matches['rect_x'].apply(lambda x: x + shift)
        matches = self.define_product_position(matches, shelf_length)
        matches_position = matches[['probe_match_fk', 'position']]
        filtered_matches = self.filtered_matches.merge(matches_position, on='probe_match_fk', how='left')
        result_df = self.get_vertical_placement_kpi_result_df(filtered_matches)
        for i, row in result_df.iterrows():
            self.common.write_to_db_result(fk=row['kpi_fk'], numerator_id=row['product_fk'], denominator_id=self.store_id,
                                           numerator_result=row['count'], denominator_result=row['total_facings'],
                                           result=row['ratio'], score=row['ratio'])

    def define_product_position(self, matches, shelf_length):
        matches['position'] = ''
        matches.loc[(matches['adjusted_rect_x'] >= 0) &
                    (matches['adjusted_rect_x'] <= (shelf_length / 3)), 'position'] = self.SHELF_PLACEMENT_VERTICAL_LEFT
        matches.loc[(matches['adjusted_rect_x'] > (shelf_length / 3)) &
                    (matches['adjusted_rect_x'] <= (shelf_length * 2 / 3)), 'position'] = self.SHELF_PLACEMENT_VERTICAL_CENTER
        matches.loc[(matches['adjusted_rect_x'] > (shelf_length * 2 / 3)) &
                    (matches['adjusted_rect_x'] <= shelf_length), 'position'] = self.SHELF_PLACEMENT_VERTICAL_RIGHT
        return matches

    def get_vertical_placement_kpi_result_df(self, filtered_matches):
        all_products_df = filtered_matches.groupby(['product_fk'], as_index=False).agg({'count': np.sum})
        all_products_df.rename({'count': 'total_facings'})
        result_df = filtered_matches.groupby(['product_fk', 'position'], as_index=False).agg({'count': np.sum})
        result_df = result_df.merge(all_products_df, on='product_fk', how='left')
        result_df['ratio'] = result_df['count'] / result_df['total_facings']
        result_df['kpi_fk'] = result_df['position'].apply(lambda x: self.common.get_kpi_fk_by_kpi_type(x))
        return result_df

    # def calculate_shelf_placement_vertical(self):
    #     bay_length_df = self.match_product_in_scene.groupby(['bay_number'], as_index=False).agg({'shelf_px_total': np.max})
    #          # i think groupby sorts automatically
    #     bay_length_df = self.get_adjusted_length(bay_length_df)
    #     adjusted_matches = self.match_product_in_scene.merge(bay_length_df, on='bay_number', how='left')
    #     adjusted_matches['adjusted_shelf_px_left'] = adjusted_matches['shelf_px_left']+adjusted_matches['accum_length_shifted']
    #     adjusted_matches['adjusted_shelf_px_right'] = adjusted_matches['shelf_px_right']+adjusted_matches['accum_length_shifted']
    #     positions_df = self.create_position_borders_dict(bay_length_df)
    #
    #
    # @staticmethod
    # def create_position_borders_dict(bay_length_df):
    #     scene_length = float(bay_length_df['accum_length'].max())
    #     positions_df = pd.DataFrame(columns=['l_border', 'r_border', 'position'])
    #     positions_df.append({'l_border': 0, 'r_border': scene_length / 3, 'position': 'Left'})
    #     positions_df.append({'l_border': scene_length / 3, 'r_border': scene_length * 2 / 3, 'position': 'Center'})
    #     positions_df.append({'l_border': scene_length * 2 / 3, 'r_border': scene_length, 'position': 'Right'})
    #     return positions_df
    #
    # @staticmethod
    # def get_adjusted_length(bay_length_df):
    #     bay_length_df = bay_length_df.sort_values(['bay_number'])
    #     #Option 1
    #     # bay_length_df['accum_length'] = bay_length_df['shelf_px_total']
    #     # for i, row in bay_length_df.iterrows():
    #     #     current_bay = row['bay_number']
    #     #     if current_bay > 1:
    #     #         previous_bay_adj_len = bay_length_df[bay_length_df['bay_number'] == current_bay-1]['accum_length'].values[0]
    #     #         bay_length_df.loc[bay_length_df['bay_number'] == current_bay, 'accum_length'] \
    #     #             = row['shelf_px_total'] + previous_bay_adj_len
    #     #Option 2 - check if the results are the same
    #     bay_length_df['accum_length'] = bay_length_df.groupby('shelf_px_total').cumsum()
    #     bay_length_df['accum_length_shifted'] = bay_length_df['accum_length'].shift(1)
    #     bay_length_df.rename(columns={'shelf_px_total': 'shelf_max'})
    #     return bay_length_df

    def calculate_product_blocking(self):
        external_targets = self.commontools.all_targets_unpacked[self.commontools.all_targets_unpacked['type']
                                                                        == self.PRODUCT_BLOCKING]
        additional_block_params = {'check_vertical_horizontal': True, 'minimum_facing_for_block': 3,
                                   'include_stacking': True,
                                   'allowed_products_filters': {'product_type': ['Empty']}}
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.PRODUCT_BLOCKING)

        for i, row in external_targets.iterrows():
            group_fk = self.custom_entities[self.custom_entities['name'] == row['Group Name']]
            filters = self.get_block_and_adjacency_filters(row)
            target = row['Target'] # check case in temaplate
            additional_block_params.update({'minimum_block_ratio': target})
            if self.excluded_matches:
                filters = self.adjust_filters_and_data_provider_for_calculations(filters, self.block)

            result_df = self.block.network_x_block_together(filters, additional=additional_block_params)
            score = max_ratio = 0
            result = self.commontools.get_yes_no_result(0)
            if not result_df.empty:
                result_df = result_df[result_df['is_block'] == True]
                if not result_df.empty:
                    max_ratio = result_df['facing_percentage'].max()
                    result_df = result_df[result_df['facing_percentage'] == max_ratio]
                    result = self.commontools.get_yes_no_result(1)
                    orientation = result_df['orientation'].values[0]
                    score = self.commontools.get_kpi_result_value_pk_by_value(orientation.upper())
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=group_fk, denominator_id=self.store_id,
                                           numerator_result=max_ratio,
                                           score=score, result=result, target=target, by_scene=True)

            self.block_results.append({'Group Name': row['Group Name'], 'Score': score})
            if self.excluded_matches:
                self.block.data_provider._set_matches(self.match_product_in_scene)

    @staticmethod
    def get_block_and_adjacency_filters(target_series):
        filters = {target_series['Parameter 1']: target_series['Value 1']}
        if target_series['Parameter 2']:
            filters.update({target_series['Parameter 2']: target_series['Value 2']})
        return filters

    def adjust_filters_and_data_provider_for_calculations(self, filters, instance):
        # matches = matches_df.copy()
        matches = self.block.data_provider[Data.MATCHES] if isinstance(instance, Block) \
                                                else self.adjacency.data_provider[Data.MATCHES]
        matches_products = pd.merge(self.filtered_matches, self.all_products, on='product_fk', how='left')
        output_filters = {}
        for field, value in filters.items():
            add_field = '{}_add'.format(field)
            new_filter = {add_field: value}
            if add_field not in matches.columns.values.tolist():
                matches[add_field] = 'N/A'
            included_matches = set(matches_products[self.toolbox.get_filter_condition \
                                                        (matches_products, **{field: value})]['probe_match_fk'].values.tolist())
            matches.loc[matches['probe_match_fk'].isin(included_matches), add_field] = value
            output_filters.update(new_filter)

        if isinstance(instance, Block):
            self.block.data_provider._set_matches(matches)
        else:
            self.adjacency.data_provider._set_matches(matches)
        return output_filters

    def calculate_adjacency(self):
        block_pairs = self.get_group_pairs()
        if block_pairs:
            external_targets = self.commontools.all_targets_unpacked[self.commontools.all_targets_unpacked['type']
                                                                     == self.PRODUCT_BLOCKING]
            additional_block_params = {'check_vertical_horizontal': True, 'minimum_facing_for_block': 3,
                                       'minimum_block_ratio': 0.9, 'include_stacking': True,
                                       'allowed_products_filters': {'product_type': ['Empty']}}
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.PRODUCT_BLOCKING_ADJACENCY)

            for pair in block_pairs:
                pair = list(pair)
                group_1_fk = self.custom_entities[self.custom_entities['name'] == pair[0]]
                group_2_fk = self.custom_entities[self.custom_entities['name'] == pair[1]]

                group_1_targets = external_targets[external_targets['Group Name'] == pair[0]].iloc[0]
                group_1_filters = self.get_block_and_adjacency_filters(group_1_targets)

                group_2_targets = external_targets[external_targets['Group Name'] == pair[1]].iloc[0]
                group_2_filters = self.get_block_and_adjacency_filters(group_2_targets)

                if self.excluded_matches:
                    group_1_filters = self.adjust_filters_and_data_provider_for_calculations(group_1_filters, self.adjacency)
                    group_2_filters = self.adjust_filters_and_data_provider_for_calculations(group_2_filters, self.adjacency)

                result_df = self.adjacency.network_x_adjacency_calculation({'anchor_products': group_1_filters,
                                                                            'tested_products': group_2_filters},
                                                                           additional=additional_block_params)
                score = 0
                result = self.commontools.get_yes_no_result(0)
                if not result_df.empty:
                    score = 1 if result_df['is_adj'].values[0] else 0
                    result = self.commontools.get_yes_no_result(score)
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=group_1_fk, denominator_id=group_2_fk,
                                               result=result, score=score, by_scene=True)

                if self.excluded_matches:
                    self.adjacency.data_provider._set_matches(self.match_product_in_scene)

    def get_group_pairs(self):
        valid_groups = self.block_results[self.block_results['Score'] == 1]['Group Name'].values.tolist()
        result_set = set()
        for i, group in enumerate(valid_groups):
            [result_set.add(frozenset([group, valid_groups[j]])) for j in range(i+1, len(valid_groups))]
        return list(result_set)

    def get_scene_bay_max_shelves(self, shelf_placement_targets):
        scene_bay_max_shelves = self.match_product_in_scene.groupby(['bay_number'],
                                                                    as_index=False).agg({'shelf_number_from_bottom':np.max})
        scene_bay_max_shelves.rename({'shelf_number_from_bottom': 'shelves_in_bay'})
        max_shelf_in_template = shelf_placement_targets['No of Shelves in Fixture (per bay) (key)'].max()
        scene_bay_max_shelves = scene_bay_max_shelves.merge(shelf_placement_targets, left_on='shelves_in_bay',
                                                            right_on='No of Shelves in Fixture (per bay) (key)')
        scene_bay_max_shelves = self.complete_missing_target_shelves(scene_bay_max_shelves, max_shelf_in_template)
        scene_bay_max_shelves['shelves_all_placements'] = scene_bay_max_shelves.groupby(['bay_number']) \
                                            ['Shelves From Bottom To Include (data)'].apply(lambda x: ','.join(str(x)))
        # relevant_scenes = self.filtered_matches['scene_fk'].unique().tolist()
        scene_bay_max_shelves = scene_bay_max_shelves[~(scene_bay_max_shelves['bay_number'] == -1)]
        final_df = pd.DataFrame(columns=scene_bay_max_shelves.columns.value.tolist())
        relevant_bays = self.filtered_matches['bay_number'].values.tolist()
        for i, row in scene_bay_max_shelves.iterrows():
            if row['bay_number'] in relevant_bays:
                final_df.append(row)
        return final_df

    def complete_missing_target_shelves(self, scene_bay_df, max_shelf_template):
        for i, row in scene_bay_df.iterrows():
            if row['shelves_in_bay'] > max_shelf_template:
                scene_bay_df.loc[(scene_bay_df['bay_number'] == row['bay_number']) &
                                 (scene_bay_df['type'] == self.PLACEMENT_BY_SHELF_NUMBERS_TOP),
                                 'Shelves From Bottom To Include (data)'] = row['shelves_in_bay']
                scene_bay_df.loc[(scene_bay_df['bay_number'] == row['bay_number']) &
                                 (scene_bay_df['type'] == self.PLACEMENT_BY_SHELF_NUMBERS_TOP),
                                 'No of Shelves in Fixture (per bay) (key)'] = row['shelves_in_bay']
        scene_bay_df = scene_bay_df[~scene_bay_df['Shelves From Bottom To Include (data)'].isnull()]
        return scene_bay_df

    def filter_out_irrelevant_matches(self, target_kpis_df):
        relevant_matches = self.scene_bay_shelf_product[~(self.scene_bay_shelf_product['bay_number'] == -1)]
        for i, row in target_kpis_df.iterrows():
            all_shelves = map(lambda x: float(x), row['shelves_all_placements'].split(','))
            rows_to_remove = relevant_matches[(relevant_matches['bay_number'] == row['bay_number']) &
                                              (~(relevant_matches['shelf_number'].isin(all_shelves)))].index
            relevant_matches.drop(rows_to_remove, inplace=True)
        relevant_matches['position'] = ''
        return relevant_matches

    def get_kpi_results_df(self, relevant_matches, kpi_targets_df):
        total_products_facings = relevant_matches.groupby(['product_fk'], as_index=False).agg({'count': np.sum})
        total_products_facings.rename(columns={'count': 'total_facings'})
        # result_df = pd.pivot_table(relevant_matches, index=['product_fk'], columns=['type'], values='count',
        #                            aggfunc=np.sum)
        # result_df = result_df.reset_index()
        result_df = relevant_matches.groupby(['product_fk', 'type'], as_index=False).agg({'count':np.sum})
        result_df.merge(total_products_facings, on='product_fk', how='left')

        kpis_df = kpi_targets_df.drop_duplicates(subset=['kpi_level_2_fk', 'type', 'KPI Parent'])
        result_df = result_df.merge(kpis_df, on='type', how='left')
        # result_df['identifier_parent'] = result_df['KPI Parent'].apply(lambda x:
        #                                                                self.common.get_dictionary(
        #                                                                kpi_fk=int(float(x)))) # looks like no need for parent
        result_df['ratio'] = result_df.apply(self.get_sku_ratio, axis=1)
        return result_df

    def get_sku_ratio(self, row):
        ratio = row['count'] / row['total_facings']
        return ratio