
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
from Projects.PEPSICOUK.Utils.Fetcher import PEPSICOUK_Queries
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency
from KPIUtils_v2.Calculations.BlockCalculations import Block

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
    SHELF_PLACEMENT_VERTICAL_LEFT = 'Shelf Placement Vertical_Left'
    SHELF_PLACEMENT_VERTICAL_CENTER = 'Shelf Placement Vertical_Center'
    SHELF_PLACEMENT_VERTICAL_RIGHT = 'Shelf Placement Vertical_Right'
    PRODUCT_BLOCKING = 'Product Blocking'
    PRODUCT_BLOCKING_ADJACENCY = 'Product Blocking Adjacency'
    PRIMARY_SHELF = 'Primary Shelf'
    NUMBER_OF_SHELVES_TEMPL_COLUMN = 'No of Shelves in Fixture (per bay) (key)'
    RELEVANT_SHELVES_TEMPL_COLUMN = 'Shelves From Bottom To Include (data)'
    SHELF_PLC_TARGETS_COLUMNS = ['kpi_operation_type_fk', 'operation_type', 'kpi_level_2_fk', 'type',
                                 NUMBER_OF_SHELVES_TEMPL_COLUMN, RELEVANT_SHELVES_TEMPL_COLUMN, 'KPI Parent']
    SHELF_PLC_TARGET_COL_RENAME = {'kpi_operation_type_fk_x': 'kpi_operation_type_fk',
                                   'operation_type_x': 'operation_type', 'kpi_level_2_fk_x': 'kpi_level_2_fk',
                                   'type_x': 'type', NUMBER_OF_SHELVES_TEMPL_COLUMN+'_x': NUMBER_OF_SHELVES_TEMPL_COLUMN,
                                   RELEVANT_SHELVES_TEMPL_COLUMN+'_x': RELEVANT_SHELVES_TEMPL_COLUMN,
                                   'KPI Parent_x':  'KPI Parent'}
    
    def __init__(self, data_provider, output, common=None):
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
        self.store_id = self.data_provider[Data.STORE_FK] if self.data_provider[Data.STORE_FK] is not None \
                                                            else self.session_info['store_fk'].values[0]
        self.all_templates = self.data_provider[Data.ALL_TEMPLATES]
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]

        self.probe_groups = self.get_probe_group()
        self.match_product_in_scene = self.match_product_in_scene.merge(self.probe_groups, on='probe_match_fk',
                                                                        how='left')
        self.is_solid_scene = True if len(self.probe_groups['probe_group_id'].unique().tolist()) <= 1 else False
        self.toolbox = GENERALToolBox(self.data_provider)
        self.commontools = PEPSICOUKCommonToolBox(self.data_provider, self.rds_conn)

        self.custom_entities = self.commontools.custom_entities
        self.on_display_products = self.commontools.on_display_products
        self.exclusion_template = self.commontools.exclusion_template
        self.filtered_scif = self.commontools.filtered_scif
        self.filtered_matches = self.commontools.filtered_matches
        self.excluded_matches = self.compare_matches()
        self.filtered_matches = self.filtered_matches.merge(self.probe_groups, on='probe_match_fk', how='left')

        self.scene_bay_shelf_product = self.commontools.scene_bay_shelf_product
        self.external_targets = self.commontools.external_targets
        self.own_manuf_fk = self.all_products[self.all_products['manufacturer_name'] == self.PEPSICO]['manufacturer_fk'].values[0]
        self.block = Block(self.data_provider, custom_scif=self.filtered_scif, custom_matches=self.filtered_matches)
        self.adjacency = Adjancency(self.data_provider, custom_scif=self.filtered_scif, custom_matches=self.filtered_matches)
        self.block_results = pd.DataFrame(columns=['Group Name', 'Score'])
        self.kpi_results = pd.DataFrame(columns=['kpi_fk', 'numerator', 'denominator', 'result', 'score'])
        self.passed_blocks = {}

    def get_probe_group(self):
        query = PEPSICOUK_Queries.get_probe_group(self.session_uid)
        probe_group = pd.read_sql_query(query, self.rds_conn.db)
        return probe_group

    def compare_matches(self):
        initial_matches = set(self.match_product_in_scene['probe_match_fk'].values.tolist())
        filtered_matches = set(self.filtered_matches['probe_match_fk'].values.tolist())
        excluded_matches = initial_matches.difference(filtered_matches)
        return excluded_matches

    def main_function(self):
        if not self.filtered_matches.empty:
            self.calculate_internal_kpis()
            self.calculate_external_kpis()

    def calculate_external_kpis(self):
        # self.calculate_product_blocking()
        # self.calculate_adjacency()
        self.calculate_product_blocking_new()
        self.calculate_adjacency_new()

    def calculate_internal_kpis(self):
        self.calculate_number_of_facings_and_linear_space()
        self.calculate_number_of_bays_and_shelves()
        self.calculate_shelf_placement_horizontal()
        self.calculate_shelf_placement_vertical_mm()
        # self.calculate_shelf_placement_vertical()

    def calculate_shelf_placement_vertical_mm(self):
        probe_groups_list = self.probe_groups['probe_group_id'].unique().tolist()
        resulting_matches = pd.DataFrame()

        for probe_group in probe_groups_list:
            matches = self.match_product_in_scene[self.match_product_in_scene['probe_group_id'] == probe_group]
            filtered_matches = self.filtered_matches[self.filtered_matches['probe_group_id'] == probe_group]
            left_edge = self.get_left_edge_mm(matches)
            right_edge = self.get_right_edge_mm(matches)
            shelf_length = float(right_edge - left_edge)
            matches = self.define_product_position_mm(matches, shelf_length, left_edge, right_edge)
            matches_position = matches[['probe_match_fk', 'position']]
            filtered_matches = filtered_matches.merge(matches_position, on='probe_match_fk', how='left')
            if resulting_matches.empty:
                resulting_matches = filtered_matches
            else:
                resulting_matches = resulting_matches.append(filtered_matches)

        result_df = self.get_vertical_placement_kpi_result_df(resulting_matches)
        for i, row in result_df.iterrows():
            self.common.write_to_db_result(fk=row['kpi_fk'], numerator_id=row['product_fk'],
                                           denominator_id=row['product_fk'],
                                           numerator_result=row['count'], denominator_result=row['total_facings'],
                                           result=row['ratio'], score=row['ratio'], by_scene=True)
            self.add_kpi_result_to_kpi_results_df([row['kpi_fk'], row['product_fk'], row['product_fk'], row['ratio'],
                                                   row['ratio']])

    @staticmethod
    def get_left_edge_mm(matches):
        matches['left_edge_mm'] = matches['x_mm'] - matches['width_mm_advance'] / 2
        left_edge = matches['left_edge_mm'].min()
        return left_edge

    @staticmethod
    def get_right_edge_mm(matches):
        matches['right_edge_mm'] = matches['x_mm'] + matches['width_mm_advance'] / 2
        right_edge = matches['right_edge_mm'].max()
        return right_edge

    def define_product_position_mm(self, matches, shelf_length, left_edge, right_edge):
        matches['position'] = ''
        matches.loc[(matches['x_mm'] >= left_edge) &
                    (matches['x_mm'] <= (left_edge+shelf_length / 3)), 'position'] = self.SHELF_PLACEMENT_VERTICAL_LEFT
        matches.loc[(matches['x_mm'] > (left_edge+shelf_length / 3)) &
                    (matches['x_mm'] <= (left_edge+
                            shelf_length * 2 / 3)), 'position'] = self.SHELF_PLACEMENT_VERTICAL_CENTER
        matches.loc[(matches['x_mm'] > (left_edge+shelf_length * 2 / 3)) &
                    (matches['x_mm'] <= right_edge), 'position'] = self.SHELF_PLACEMENT_VERTICAL_RIGHT
        return matches

    def calculate_number_of_facings_and_linear_space(self):
        facing_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.NUMBER_OF_FACINGS)
        linear_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.TOTAL_LINEAR_SPACE)
        filtered_scif = self.filtered_scif.copy()
        filtered_scif['facings'] = filtered_scif.apply(self.update_facings_for_cardboard_boxes, axis=1)
        for i, row in filtered_scif.iterrows():
            self.common.write_to_db_result(fk=facing_kpi_fk, numerator_id=row['product_fk'], result=row['facings'],
                                           denominator_id=self.store_id, by_scene=True)
            self.common.write_to_db_result(fk=linear_kpi_fk, numerator_id=row['product_fk'], denominator_id=self.store_id,
                                           result=row['gross_len_add_stack'], by_scene=True)
            self.add_kpi_result_to_kpi_results_df([facing_kpi_fk, row['product_fk'], self.store_id, row['facings'], None])
            self.add_kpi_result_to_kpi_results_df(
                [linear_kpi_fk, row['product_fk'], self.store_id, row['gross_len_add_stack'], None])

    @staticmethod
    def update_facings_for_cardboard_boxes(row):
        facings = row['facings'] * 3 if row['att1'] == 'display cardboard box' else row['facings']
        return facings

    def calculate_number_of_bays_and_shelves(self):
        bays_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.NUMBER_OF_BAYS)
        shelves_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.NUMBER_OF_SHELVES)
        matches = self.match_product_in_scene[~(self.match_product_in_scene['bay_number'] == -1)]

        bays_in_scene = matches['bay_number'].unique().tolist()
        bays_num = len(bays_in_scene)
        # bay_shelf = matches.drop_duplicates(subset=['bay_number', 'shelf_number'])
        # shelf_num = len(bay_shelf)
        shelf_num = matches['shelf_number'].max()
        self.common.write_to_db_result(fk=bays_kpi_fk, numerator_id=self.own_manuf_fk, result=bays_num,
                                       denominator_id=self.store_id, by_scene=True)
        self.common.write_to_db_result(fk=shelves_kpi_fk, numerator_id=self.own_manuf_fk, result=shelf_num,
                                       denominator_id=self.store_id, by_scene=True)
        self.add_kpi_result_to_kpi_results_df([bays_kpi_fk, self.own_manuf_fk, self.store_id, bays_num, None])
        self.add_kpi_result_to_kpi_results_df(
            [shelves_kpi_fk, self.own_manuf_fk, self.store_id, shelf_num, None])

    def calculate_shelf_placement_horizontal(self):
        # shelf_placement_targets = self.commontools.get_shelf_placement_kpi_targets_data()
        external_targets = self.commontools.all_targets_unpacked
        shelf_placmnt_targets = external_targets[external_targets['operation_type'] == self.SHELF_PLACEMENT]
        if not shelf_placmnt_targets.empty:
            bay_max_shelves = self.get_scene_bay_max_shelves(shelf_placmnt_targets)
            bay_all_shelves = bay_max_shelves.drop_duplicates(subset=['bay_number', 'shelves_all_placements'],
                                                              keep='first')
            relevant_matches = self.filter_out_irrelevant_matches(bay_all_shelves)
            if not relevant_matches.empty:
                for i, row in bay_max_shelves.iterrows():
                    shelf_list = map(lambda x: float(x), row['Shelves From Bottom To Include (data)'].split(','))
                    relevant_matches.loc[(relevant_matches['bay_number'] == row['bay_number']) &
                                         (relevant_matches['shelf_number_from_bottom'].isin(shelf_list)), 'position'] = row['type']
                kpi_results = self.get_kpi_results_df(relevant_matches, bay_max_shelves)
                for i, result in kpi_results.iterrows():
                    self.common.write_to_db_result(fk=result['kpi_level_2_fk'], numerator_id=result['product_fk'],
                                                   denominator_id=result['product_fk'],
                                                   denominator_result=result['total_facings'],
                                                   numerator_result=result['count'], result=result['ratio'],
                                                   score=result['ratio'], by_scene=True)
                    self.add_kpi_result_to_kpi_results_df([result['kpi_level_2_fk'], result['product_fk'],
                                                           result['product_fk'], result['ratio'], result['ratio']])

    def calculate_shelf_placement_vertical(self):
        probe_groups_list = self.probe_groups['probe_group_id'].unique().tolist()
        resulting_matches = pd.DataFrame()

        for probe_group in probe_groups_list:
            matches = self.match_product_in_scene[self.match_product_in_scene['probe_group_id'] == probe_group]
            filtered_matches = self.filtered_matches[self.filtered_matches['probe_group_id'] == probe_group]
            left_edge = matches['rect_x'].min()
            right_edge = matches['rect_x'].max()
            shelf_length = float(right_edge - left_edge)
            matches = self.define_product_position_px(matches, shelf_length, left_edge, right_edge)
            matches_position = matches[['probe_match_fk', 'position']]
            filtered_matches = filtered_matches.merge(matches_position, on='probe_match_fk', how='left')
            if resulting_matches.empty:
                resulting_matches = filtered_matches
            else:
                resulting_matches = resulting_matches.append(filtered_matches)

        result_df = self.get_vertical_placement_kpi_result_df(resulting_matches)
        for i, row in result_df.iterrows():
            self.common.write_to_db_result(fk=row['kpi_fk'], numerator_id=row['product_fk'],
                                           denominator_id=row['product_fk'],
                                           numerator_result=row['count'], denominator_result=row['total_facings'],
                                           result=row['ratio'], score=row['ratio'], by_scene=True)
            self.add_kpi_result_to_kpi_results_df([row['kpi_fk'], row['product_fk'], row['product_fk'], row['ratio'],
                                                   row['ratio']])

    def define_product_position_px(self, matches, shelf_length, left_edge, right_edge):
        matches['position'] = ''
        matches.loc[(matches['rect_x'] >= left_edge) &
                    (matches['rect_x'] <= (left_edge+shelf_length / 3)), 'position'] = self.SHELF_PLACEMENT_VERTICAL_LEFT
        matches.loc[(matches['rect_x'] > (left_edge+shelf_length / 3)) &
                    (matches['rect_x'] <= (left_edge+
                            shelf_length * 2 / 3)), 'position'] = self.SHELF_PLACEMENT_VERTICAL_CENTER
        matches.loc[(matches['rect_x'] > (left_edge+shelf_length * 2 / 3)) &
                    (matches['rect_x'] <= right_edge), 'position'] = self.SHELF_PLACEMENT_VERTICAL_RIGHT
        return matches

    def get_vertical_placement_kpi_result_df(self, filtered_matches):
        all_products_df = filtered_matches.groupby(['product_fk'], as_index=False).agg({'count': np.sum})
        all_products_df.rename(columns={'count': 'total_facings'}, inplace=True)
        result_df = filtered_matches.groupby(['product_fk', 'position'], as_index=False).agg({'count': np.sum})
        result_df = result_df.merge(all_products_df, on='product_fk', how='left')
        result_df['ratio'] = result_df['count'] / result_df['total_facings'] * 100
        result_df['kpi_fk'] = result_df['position'].apply(lambda x: self.common.get_kpi_fk_by_kpi_type(x))
        return result_df

    def calculate_product_blocking_new(self):
        external_targets = self.commontools.all_targets_unpacked[self.commontools.all_targets_unpacked['type']
                                                                        == self.PRODUCT_BLOCKING]
        additional_block_params = {'check_vertical_horizontal': True, 'minimum_facing_for_block': 3,
                                   'include_stacking': True,
                                   'allowed_products_filters': {'product_type': ['Empty']}}
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.PRODUCT_BLOCKING)

        for i, row in external_targets.iterrows():
            # group_fk = self.custom_entities[self.custom_entities['name'] == row['Group Name']]
            group_fk = self.custom_entities[self.custom_entities['name'] == row['Group Name']]['pk'].values[0]
            filters = self.get_block_and_adjacency_filters(row)
            target = row['Target']
            additional_block_params.update({'minimum_block_ratio': float(target)/100})

            result_df = self.block.network_x_block_together(filters, additional=additional_block_params)
            score = max_ratio = 0
            result = self.commontools.get_yes_no_result(0)
            if not result_df.empty:
                max_ratio = result_df['facing_percentage'].max()
                result_df = result_df[result_df['is_block']==True]
                if not result_df.empty:
                    self.passed_blocks[row['Group Name']] = result_df

                    max_ratio = result_df['facing_percentage'].max()
                    result_df = result_df[result_df['facing_percentage'] == max_ratio]
                    result = self.commontools.get_yes_no_result(1)
                    orientation = result_df['orientation'].values[0]
                    score = self.commontools.get_kpi_result_value_pk_by_value(orientation.upper())
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=group_fk, denominator_id=self.store_id,
                                           numerator_result=max_ratio * 100,
                                           score=score, result=result, target=target, by_scene=True)

            self.block_results = self.block_results.append(pd.DataFrame([{'Group Name': row['Group Name'],
                                                                          'Score': result_df['is_block'].values[0]
                                                                          if not result_df.empty else False}]))
            self.add_kpi_result_to_kpi_results_df([kpi_fk, group_fk, self.store_id, result, score])

    def calculate_adjacency_new(self):
        block_pairs = self.get_group_pairs()
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.PRODUCT_BLOCKING_ADJACENCY)
        block_pairs = [list(pair) for pair in block_pairs]
        for pair in block_pairs:
            group_1_fk = self.custom_entities[self.custom_entities['name'] == pair[0]]['pk'].values[0]
            group_2_fk = self.custom_entities[self.custom_entities['name'] == pair[1]]['pk'].values[0]

            adjacency_results = pd.DataFrame(columns=['anchor_block', 'tested_block', 'anchor_facing_percentage',
                                                      'tested_facing_percentage', 'scene_fk', 'is_adj'])
            blocks = {'anchor_products': self.passed_blocks[pair[0]], 'tested_products': self.passed_blocks[pair[1]]}
            merged_blocks = self.adjacency._union_anchor_tested_blocks(blocks)
            adjacency_results = self.adjacency._is_block_adjacent(adjacency_results, merged_blocks)
            score = 0
            result = self.commontools.get_yes_no_result(0)
            if not adjacency_results.empty:
                adjacency_results = adjacency_results[adjacency_results['is_adj']==True]
            if not adjacency_results.empty:
                score = 1 if adjacency_results['is_adj'].values[0] else 0
                result = self.commontools.get_yes_no_result(score)
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=group_1_fk, denominator_id=group_2_fk,
                                           result=result, score=score, by_scene=True)
            self.add_kpi_result_to_kpi_results_df([kpi_fk, group_1_fk, group_2_fk, result, score])

    def calculate_product_blocking(self):
        external_targets = self.commontools.all_targets_unpacked[self.commontools.all_targets_unpacked['type']
                                                                        == self.PRODUCT_BLOCKING]
        additional_block_params = {'check_vertical_horizontal': True, 'minimum_facing_for_block': 3,
                                   'include_stacking': True,
                                   'allowed_products_filters': {'product_type': ['Empty']}}
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.PRODUCT_BLOCKING)

        for i, row in external_targets.iterrows():
            # group_fk = self.custom_entities[self.custom_entities['name'] == row['Group Name']]
            group_fk = self.custom_entities[self.custom_entities['name'] == row['Group Name']]['pk'].values[0]
            filters = self.get_block_and_adjacency_filters(row)
            target = row['Target']
            additional_block_params.update({'minimum_block_ratio': float(target)/100})

            result_df = self.block.network_x_block_together(filters, additional=additional_block_params)
            score = max_ratio = 0
            result = self.commontools.get_yes_no_result(0)
            if not result_df.empty:
                max_ratio = result_df['facing_percentage'].max()
                result_df = result_df[result_df['is_block']==True]
                if not result_df.empty:
                    max_ratio = result_df['facing_percentage'].max()
                    result_df = result_df[result_df['facing_percentage'] == max_ratio]
                    result = self.commontools.get_yes_no_result(1)
                    orientation = result_df['orientation'].values[0]
                    score = self.commontools.get_kpi_result_value_pk_by_value(orientation.upper())
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=group_fk, denominator_id=self.store_id,
                                           numerator_result=max_ratio * 100,
                                           score=score, result=result, target=target, by_scene=True)

            self.block_results = self.block_results.append(pd.DataFrame([{'Group Name': row['Group Name'],
                                                                          'Score': result_df['is_block'].values[0]
                                                                          if not result_df.empty else False}]))

    @staticmethod
    def get_block_and_adjacency_filters(target_series):
        filters = {target_series['Parameter 1']: target_series['Value 1']}
        if target_series['Parameter 2']:
            filters.update({target_series['Parameter 2']: target_series['Value 2']})
        if target_series['Parameter 3']:
            filters.update({target_series['Parameter 3']: target_series['Value 3']})
        return filters

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
                group_1_fk = self.custom_entities[self.custom_entities['name'] == pair[0]]['pk'].values[0]
                group_2_fk = self.custom_entities[self.custom_entities['name'] == pair[1]]['pk'].values[0]

                group_1_targets = external_targets[external_targets['Group Name'] == pair[0]].iloc[0]
                group_1_filters = self.get_block_and_adjacency_filters(group_1_targets)

                group_2_targets = external_targets[external_targets['Group Name'] == pair[1]].iloc[0]
                group_2_filters = self.get_block_and_adjacency_filters(group_2_targets)

                result_df = self.adjacency.network_x_adjacency_calculation({'anchor_products': group_1_filters,
                                                                            'tested_products': group_2_filters},
                                                                           location=None,
                                                                           additional=additional_block_params)
                score = 0
                result = self.commontools.get_yes_no_result(0)
                if not result_df.empty:
                    result_df = result_df[result_df['is_adj'] == True]
                if not result_df.empty:
                    score = 1 if result_df['is_adj'].values[0] else 0
                    result = self.commontools.get_yes_no_result(score)
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=group_1_fk, denominator_id=group_2_fk,
                                               result=result, score=score, by_scene=True)

                # if self.excluded_matches:
                #     self.adjacency.data_provider._set_matches(self.match_product_in_scene)

    def get_group_pairs(self):
        valid_groups = self.block_results[self.block_results['Score'] == 1]['Group Name'].values.tolist()
        result_set = set()
        for i, group in enumerate(valid_groups):
            [result_set.add(frozenset([group, valid_groups[j]])) for j in range(i+1, len(valid_groups))]
        return list(result_set)

    def get_scene_bay_max_shelves(self, shelf_placement_targets):
        scene_bay_max_shelves = self.match_product_in_scene.groupby(['bay_number'],
                                                                    as_index=False).agg({'shelf_number_from_bottom':np.max})
        scene_bay_max_shelves.rename(columns={'shelf_number_from_bottom': 'shelves_in_bay'}, inplace=True)
        min_shelf_in_template = shelf_placement_targets[self.NUMBER_OF_SHELVES_TEMPL_COLUMN].min() #added
        scene_bay_max_shelves = scene_bay_max_shelves[scene_bay_max_shelves['shelves_in_bay'] >= min_shelf_in_template] #added

        max_shelf_in_template = shelf_placement_targets[self.NUMBER_OF_SHELVES_TEMPL_COLUMN].max()
        shelf_placement_targets = self.complete_missing_target_shelves(scene_bay_max_shelves, max_shelf_in_template,
                                                                     shelf_placement_targets)

        scene_bay_max_shelves = scene_bay_max_shelves.merge(shelf_placement_targets, left_on='shelves_in_bay',
                                                            right_on=self.NUMBER_OF_SHELVES_TEMPL_COLUMN)
        scene_bay_max_shelves.rename(columns=self.SHELF_PLC_TARGET_COL_RENAME, inplace=True)
        scene_bay_max_shelves = scene_bay_max_shelves[self.SHELF_PLC_TARGETS_COLUMNS + ['bay_number', 'shelves_in_bay']]
        scene_bay_max_shelves = scene_bay_max_shelves.drop_duplicates()

        bay_all_relevant_shelves = self.get_bay_relevant_shelves_df(scene_bay_max_shelves)
        scene_bay_max_shelves = scene_bay_max_shelves.merge(bay_all_relevant_shelves, on='bay_number', how='left')

        scene_bay_max_shelves = scene_bay_max_shelves[~(scene_bay_max_shelves['bay_number'] == -1)]

        relevant_bays = self.filtered_matches['bay_number'].unique().tolist()
        final_df = scene_bay_max_shelves[scene_bay_max_shelves['bay_number'].isin(relevant_bays)]
        return final_df

    def get_bay_relevant_shelves_df(self, scene_bay_max_shelves):
        scene_bay_max_shelves[self.RELEVANT_SHELVES_TEMPL_COLUMN] = scene_bay_max_shelves[
            self.RELEVANT_SHELVES_TEMPL_COLUMN].astype(str)
        bay_all_relevant_shelves = scene_bay_max_shelves[
            ['bay_number', self.RELEVANT_SHELVES_TEMPL_COLUMN]].drop_duplicates()
        bay_all_relevant_shelves['shelves_all_placements'] = bay_all_relevant_shelves.groupby('bay_number') \
            [self.RELEVANT_SHELVES_TEMPL_COLUMN].apply(lambda x: (x + ',').cumsum().str.strip())
        if 'bay_number' in bay_all_relevant_shelves.index.names:
            bay_all_relevant_shelves.index.names = ['custom_ind']
        bay_all_relevant_shelves = bay_all_relevant_shelves.drop_duplicates(subset=['bay_number'], keep='last') \
            [['bay_number', 'shelves_all_placements']]
        bay_all_relevant_shelves['shelves_all_placements'] = bay_all_relevant_shelves['shelves_all_placements']. \
            apply(lambda x: x[0:-1])
        return bay_all_relevant_shelves

    def complete_missing_target_shelves(self, scene_bay_df, max_shelf_template, shelf_placement_targets):
        shelf_placement_targets = shelf_placement_targets[self.SHELF_PLC_TARGETS_COLUMNS]
        shelf_placement_targets = shelf_placement_targets.reset_index(drop=True)
        for i, row in scene_bay_df.iterrows():
            if row['shelves_in_bay'] > max_shelf_template:
                if row['shelves_in_bay'] not in shelf_placement_targets[self.NUMBER_OF_SHELVES_TEMPL_COLUMN].values.tolist():
                    rows_to_add = shelf_placement_targets[shelf_placement_targets[self.NUMBER_OF_SHELVES_TEMPL_COLUMN] \
                                                                == max_shelf_template]
                    rows_to_add[self.NUMBER_OF_SHELVES_TEMPL_COLUMN] = row['shelves_in_bay']
                    top_shelf_range = ','.join(map(lambda x: str(x),
                                                        range(int(float(max_shelf_template)), int(float(row['shelves_in_bay']+1)))))
                    rows_to_add.loc[rows_to_add['type'] == self.PLACEMENT_BY_SHELF_NUMBERS_TOP,
                                    self.RELEVANT_SHELVES_TEMPL_COLUMN] = top_shelf_range
                    shelf_placement_targets = shelf_placement_targets.append(rows_to_add, ignore_index=True)
        return shelf_placement_targets

    def filter_out_irrelevant_matches(self, target_kpis_df):
        relevant_matches = self.scene_bay_shelf_product[~(self.scene_bay_shelf_product['bay_number'] == -1)]
        relevant_matches = relevant_matches[relevant_matches['bay_number'].isin(target_kpis_df['bay_number'].unique().tolist())] # added
        for i, row in target_kpis_df.iterrows():
            all_shelves = map(lambda x: float(x), row['shelves_all_placements'].split(','))
            rows_to_remove = relevant_matches[(relevant_matches['bay_number'] == row['bay_number']) &
                                              (~(relevant_matches['shelf_number_from_bottom'].isin(all_shelves)))].index
            relevant_matches.drop(rows_to_remove, inplace=True)
        relevant_matches['position'] = ''
        return relevant_matches

    def get_kpi_results_df(self, relevant_matches, kpi_targets_df):
        total_products_facings = relevant_matches.groupby(['product_fk'], as_index=False).agg({'count': np.sum})
        total_products_facings.rename(columns={'count': 'total_facings'}, inplace=True)
        result_df = relevant_matches.groupby(['product_fk', 'position'], as_index=False).agg({'count':np.sum})
        result_df = result_df.merge(total_products_facings, on='product_fk', how='left')

        kpis_df = kpi_targets_df.drop_duplicates(subset=['kpi_level_2_fk', 'type', 'KPI Parent'])
        result_df = result_df.merge(kpis_df, left_on='position', right_on='type', how='left')
        # result_df['identifier_parent'] = result_df['KPI Parent'].apply(lambda x:
        #                                                                self.common.get_dictionary(
        #                                                                kpi_fk=int(float(x)))) # looks like no need for parent
        result_df['ratio'] = result_df.apply(self.get_sku_ratio, axis=1)
        return result_df

    def get_sku_ratio(self, row):
        ratio = float(row['count']) / row['total_facings'] * 100
        return ratio

    def add_kpi_result_to_kpi_results_df(self, result_list):
        self.kpi_results.loc[len(self.kpi_results)] = result_list