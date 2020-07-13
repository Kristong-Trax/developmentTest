from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
import numpy as np
import json

from KPIUtils_v2.DB.Common import Common as CommonV1
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Projects.PEPSICOUK.Utils.Fetcher import PEPSICOUK_Queries
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts, MatchesConsts
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

__author__ = 'natalyak'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class PEPSICOUKCommonToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUSION_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                           'Inclusion_Exclusion_Template_Rollout.xlsx')
    DISPLAY_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                         'display_template.xlsx')
    ADDITIONAL_DISPLAY = 'additional display'
    STOCK = 'stock'
    INCLUDE_EMPTY = True
    EXCLUDE_EMPTY = False
    OPERATION_TYPES = []

    SOS_VS_TARGET = 'SOS vs Target'
    HERO_SKU_SPACE_TO_SALES_INDEX = 'Hero SKU Space to Sales Index'
    HERO_SKU_SOS_VS_TARGET = 'Hero SKU SOS vs Target'
    LINEAR_SOS_INDEX = 'Linear SOS Index'
    PEPSICO = 'PEPSICO'
    SHELF_PLACEMENT = 'Shelf Placement'
    HERO_SKU_PLACEMENT_TOP = 'Hero SKU Placement by shelf numbers_Top'
    HERO_PLACEMENT = 'Hero Placement'
    HERO_SKU_STACKING = 'Hero SKU Stacking'
    HERO_SKU_PRICE = 'Hero SKU Price'
    HERO_SKU_PROMO_PRICE = 'Hero SKU Promo Price'
    BRAND_FULL_BAY_KPIS = ['Brand Full Bay 90', 'Brand Full Bay 100']
    ALL = 'ALL'
    DISPLAY_NAME_TEMPL = 'Display Name'
    KPI_LOGIC = 'KPI Logic'
    SHELF_LEN_DISPL = 'Shelf Length, m'
    BAY_TO_SEPARATE = 'use bay to separate display'
    BIN_TO_SEPARATE = 'use bin to separate display'

    def __init__(self, data_provider, rds_conn=None):
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES] # initial matches
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.all_templates = self.data_provider[Data.ALL_TEMPLATES]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS] # initial scif
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng) if rds_conn is None else rds_conn
        self.complete_scif_data()
        self.store_areas = self.get_store_areas()
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.full_store_info = self.get_store_data_by_store_id()
        self.store_info_dict = self.full_store_info.to_dict('records')[0]
        self.store_policy_exclusion_template = self.get_store_policy_data_for_exclusion_template()
        self.displays_template = self.get_display_parameters()

        self.toolbox = GENERALToolBox(data_provider)
        self.custom_entities = self.get_custom_entity_data()
        self.on_display_products = self.get_on_display_products()
        self.exclusion_template = self.get_exclusion_template_data()
        self.filtered_scif = self.scif # filtered scif acording to exclusion template
        self.filtered_matches = self.match_product_in_scene # filtered scif according to exclusion template
        self.set_filtered_scif_and_matches_for_all_kpis(self.scif, self.match_product_in_scene)

        self.scene_bay_shelf_product = self.get_facings_scene_bay_shelf_product()
        self.external_targets = self.get_all_kpi_external_targets()
        self.all_targets_unpacked = self.unpack_all_external_targets()
        self.kpi_result_values = self.get_kpi_result_values_df()
        self.kpi_score_values = self.get_kpi_score_values_df()

        # self.displays_template = self.get_display_parameters()
        self.full_pallet_len = self.displays_template[self.displays_template[self.DISPLAY_NAME_TEMPL] == \
                                                      'HO Agreed Full Pallet'][self.SHELF_LEN_DISPL].values[0]
        self.half_pallet_len = self.displays_template[self.displays_template[self.DISPLAY_NAME_TEMPL] == \
                                                      'HO Agreed Half Pallet'][self.SHELF_LEN_DISPL].values[0]
        self.shelf_len_mixed_shelves = self.calculate_shelf_len_for_mixed_shelves()
        self.scene_display = self.get_match_display_in_scene()
        self.are_all_bins_tagged = self.check_if_all_bins_are_recognized()
        self.filtered_scif_secondary = self.get_initial_secondary_scif()
        self.filtered_matches_secondary = self.get_initial_secondary_matches()
        if self.are_all_bins_tagged:
            self.assign_bays_to_bins()
            self.set_filtered_scif_and_matches_for_all_kpis_secondary(self.filtered_scif_secondary,
                                                                      self.filtered_matches_secondary)

    def check_if_all_bins_are_recognized(self):
        tasks_with_bin_logic =  self.displays_template[(self.displays_template[self.KPI_LOGIC] == 'Bin') &
                                                  (self.displays_template[self.BAY_TO_SEPARATE] == 'No') &
                                                  (self.displays_template[self.BIN_TO_SEPARATE] == 'Yes')] \
            [self.DISPLAY_NAME_TEMPL].unique()
        scenes_with_bin_logic = set(self.scif[self.scif[ScifConsts.TEMPLATE_NAME].isin(tasks_with_bin_logic)]\
            [ScifConsts.SCENE_FK].unique())
        scenes_with_tagged_bins = set(self.scene_display[ScifConsts.SCENE_FK].unique()) if \
            len(self.scene_display[ScifConsts.SCENE_FK].unique())>0 else set([0])
        missing_bin_tags = scenes_with_bin_logic.difference(scenes_with_tagged_bins)
        flag = False if missing_bin_tags else True
        return flag

    def add_sub_category_to_empty_and_other(self, scif, matches):
        # exclude bin scenes
        matches['include'] = 1
        bin_scenes = self.displays_template[self.displays_template[self.KPI_LOGIC] == 'Bin'] \
            [self.DISPLAY_NAME_TEMPL].values
        scenes_excluding_bins = scif[~scif['template_name'].isin(bin_scenes)][ScifConsts.SCENE_FK].unique()
        matches.loc[~matches[MatchesConsts.SCENE_FK].isin(scenes_excluding_bins), 'include'] = 0

        mix_scenes = self.displays_template[self.displays_template[self.KPI_LOGIC] == 'Mix'] \
            [self.DISPLAY_NAME_TEMPL].values
        mix_scenes = scif[scif['template_name'].isin(mix_scenes)][ScifConsts.SCENE_FK].unique()

        products_df = self.all_products[[ScifConsts.PRODUCT_FK, ScifConsts.SUB_CATEGORY_FK, ScifConsts.PRODUCT_TYPE,
                                         ScifConsts.BRAND_FK, ScifConsts.CATEGORY_FK]]
        matches = matches.merge(products_df, on=MatchesConsts.PRODUCT_FK, how='left')
        matches['count_sub_cat'] = 1
        scene_bay_shelves = self.match_product_in_scene.groupby([MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER],
                                            as_index=False).agg({MatchesConsts.SHELF_NUMBER: np.max})
        scene_bay_shelves.rename(columns={MatchesConsts.SHELF_NUMBER: 'max_shelf'}, inplace=True)

        matches = matches.merge(scene_bay_shelves, on=[MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER],
                                how='left')
        matches.loc[(matches[MatchesConsts.SCENE_FK].isin(mix_scenes)) &
                    (matches[MatchesConsts.SHELF_NUMBER] == matches['max_shelf']), 'include'] = 0

        matches_sum = matches[matches['include'] == 1]
        scene_bay_sub_cat_sum = matches_sum.groupby([MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER,
                                                     ScifConsts.SUB_CATEGORY_FK], as_index=False).agg(
            {'count_sub_cat': np.sum})
        scene_bay_sub_cat = scene_bay_sub_cat_sum.sort_values(by=[MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER,
                                                                  'count_sub_cat'])
        scene_bay_sub_cat = scene_bay_sub_cat.drop_duplicates(subset=[MatchesConsts.SCENE_FK,
                                                                      MatchesConsts.BAY_NUMBER], keep='last')
        scene_bay_sub_cat.rename(columns={ScifConsts.SUB_CATEGORY_FK: 'max_sub_cat',
                                          'count_sub_cat': 'max_sub_cat_facings'}, inplace=True)

        matches = matches.merge(scene_bay_sub_cat, on=[MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER], how='left')
        matches.loc[((matches[ScifConsts.PRODUCT_TYPE].isin(['Empty', 'Other'])) &
                     (matches['include'] == 1)), ScifConsts.SUB_CATEGORY_FK] = \
            matches['max_sub_cat']
        matches['shelves_bay_before'] = None
        matches['shelves_bay_after'] = None
        if not matches.empty:
            matches['shelves_bay_before'] = matches.apply(self.get_shelves_for_bay, args=(scene_bay_shelves, -1),
                                                          axis=1)
            matches['shelves_bay_after'] = matches.apply(self.get_shelves_for_bay, args=(scene_bay_shelves, 1), axis=1)
            matches[ScifConsts.SUB_CATEGORY_FK] = matches.apply(self.get_subcategory_from_neighbour_bays,
                                                                args=(scene_bay_sub_cat_sum,), axis=1)

        matches = matches.drop(columns=[ScifConsts.PRODUCT_TYPE, ScifConsts.BRAND_FK, ScifConsts.CATEGORY_FK])
        return scif, matches

    def get_shelves_for_bay(self, row, scene_bay_shelves, bay_diff):
        max_shelves_df = scene_bay_shelves[
            (scene_bay_shelves[MatchesConsts.BAY_NUMBER] == row[MatchesConsts.BAY_NUMBER] + bay_diff) &
            (scene_bay_shelves[MatchesConsts.SCENE_FK] == row[MatchesConsts.SCENE_FK])]
        max_shelves = max_shelves_df['max_shelf'].values[0] if not max_shelves_df.empty else None
        return max_shelves

    def get_subcategory_from_neighbour_bays(self, row, scene_bay_sub_cat_sum):
        subcat = row[ScifConsts.SUB_CATEGORY_FK]
        if row['include'] == 1:
            if (str(subcat) == 'nan' or subcat is None) and (row[ScifConsts.PRODUCT_TYPE] in ['Empty', 'Other']):
                if (row['shelves_bay_before'] == row['max_shelf'] and row['shelves_bay_after'] == row['max_shelf']) or \
                        (row['shelves_bay_before'] != row['max_shelf'] and row['shelves_bay_after'] != row[
                            'max_shelf']):
                    reduced_df = scene_bay_sub_cat_sum[(scene_bay_sub_cat_sum[MatchesConsts.BAY_NUMBER].
                        isin([row[MatchesConsts.BAY_NUMBER] + 1, row[MatchesConsts.BAY_NUMBER] - 1])) &
                                                       (scene_bay_sub_cat_sum[ScifConsts.SCENE_FK] == row[
                                                           ScifConsts.SCENE_FK])]
                    subcat_df = reduced_df.groupby([ScifConsts.SUB_CATEGORY_FK], as_index=False).agg(
                        {'count_sub_cat': np.sum})
                    subcat = subcat_df[subcat_df['count_sub_cat'] == subcat_df['count_sub_cat'].max()] \
                        [ScifConsts.SUB_CATEGORY_FK].values[0] if not subcat_df.empty else None
                elif row['shelves_bay_before'] == row['max_shelf']:
                    reduced_df = scene_bay_sub_cat_sum[(scene_bay_sub_cat_sum[MatchesConsts.BAY_NUMBER].
                        isin([row[MatchesConsts.BAY_NUMBER] - 1])) &
                                                       (scene_bay_sub_cat_sum[ScifConsts.SCENE_FK] == row[
                                                           ScifConsts.SCENE_FK])]
                    subcat_df = reduced_df.groupby([ScifConsts.SUB_CATEGORY_FK], as_index=False).agg(
                        {'count_sub_cat': np.sum})
                    subcat = subcat_df[subcat_df['count_sub_cat'] == subcat_df['count_sub_cat'].max()] \
                        [ScifConsts.SUB_CATEGORY_FK].values[0] if not subcat_df.empty else None
                elif row['shelves_bay_after'] == row['max_shelf']:
                    reduced_df = scene_bay_sub_cat_sum[(scene_bay_sub_cat_sum[MatchesConsts.BAY_NUMBER].
                        isin([row[MatchesConsts.BAY_NUMBER] + 1])) &
                                                       (scene_bay_sub_cat_sum[ScifConsts.SCENE_FK] == row[
                                                           ScifConsts.SCENE_FK])]
                    subcat_df = reduced_df.groupby([ScifConsts.SUB_CATEGORY_FK], as_index=False).agg(
                        {'count_sub_cat': np.sum})
                    subcat = subcat_df[subcat_df['count_sub_cat'] == subcat_df['count_sub_cat'].max()] \
                        [ScifConsts.SUB_CATEGORY_FK].values[0] if not subcat_df.empty else None
                else:
                    subcat = None
                return subcat
            else:
                return subcat
        return subcat

    def get_store_areas(self):
        query = PEPSICOUK_Queries.get_all_store_areas()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def calculate_shelf_len_for_mixed_shelves(self):
        mix_displays = self.displays_template[self.displays_template[self.KPI_LOGIC] == 'Mix']\
            [self.DISPLAY_NAME_TEMPL].unique()
        mix_scenes = self.scif[self.scif[ScifConsts.TEMPLATE_NAME].isin(mix_displays)][ScifConsts.SCENE_FK].unique()
        mix_matches = self.match_product_in_scene[self.match_product_in_scene[MatchesConsts.SCENE_FK].isin(mix_scenes)]
        shelf_len_df = pd.DataFrame(columns=[MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER, 'shelf_length'])
        if not mix_matches.empty:
            shelf_len_df[MatchesConsts.SCENE_FK] = shelf_len_df[MatchesConsts.SCENE_FK].astype('float')
            shelf_len_df[MatchesConsts.BAY_NUMBER] = shelf_len_df[MatchesConsts.BAY_NUMBER].astype('float')
            scenes_bays = mix_matches.drop_duplicates(subset=[MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER])
            for i, row in scenes_bays.iterrows():
                filtered_matches = mix_matches[(mix_matches[MatchesConsts.SCENE_FK]==row[MatchesConsts.SCENE_FK]) &
                                               (mix_matches[MatchesConsts.BAY_NUMBER] == row[MatchesConsts.BAY_NUMBER])]
                left_edge = self.get_left_edge_mm(filtered_matches)
                right_edge = self.get_right_edge_mm(filtered_matches)
                shelf_len = float(right_edge - left_edge)
                shelf_len_df.loc[len(shelf_len_df)] = [row[MatchesConsts.SCENE_FK], row[MatchesConsts.BAY_NUMBER], shelf_len]
        return shelf_len_df

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

    def get_match_display_in_scene(self):
        query = PEPSICOUK_Queries.get_match_display(self.session_uid)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_display_parameters(self):
        display_templ = pd.read_excel(self.DISPLAY_TEMPLATE_PATH)
        display_templ = display_templ.fillna('')
        display_templ[self.SHELF_LEN_DISPL] = display_templ[self.SHELF_LEN_DISPL].apply(lambda x: x*1000 if x!='N/A'
                                                                                                  else 0)
        return display_templ

    def recalculate_display_product_length(self, scif, matches):
        scene_template = scif[[ScifConsts.SCENE_FK, ScifConsts.TEMPLATE_NAME]].drop_duplicates()
        matches = matches.merge(scene_template, on=ScifConsts.SCENE_FK, how='left')
        scif = scif.merge(self.displays_template, left_on=ScifConsts.TEMPLATE_NAME, right_on=self.DISPLAY_NAME_TEMPL,
                          how='left')
        matches = matches.merge(self.displays_template, left_on=ScifConsts.TEMPLATE_NAME,
                                right_on=self.DISPLAY_NAME_TEMPL, how='left')
        matches['facings_matches'] = 1
        if not matches.empty:
            matches = self.place_posms_at_bay_minus_one_to_bays(matches)
            # matches = self.construct_display_id(matches)
            max_display_id = matches['display_id'].max()

            bin_bay_scif, bin_bay_matches = self.calculate_displays_by_bin_bay_logic(scif, matches)
            bay_scif, bay_matches, max_display_id = self.calculate_displays_separated_by_bays(scif, matches,
                                                                                              max_display_id)
            bin_bin_scif, bin_bin_matches = self.calculate_displays_by_bin_bin_logic(scif, matches, max_display_id)
            bin_shelf_scif, bin_shelf_matches = self.calculate_displays_by_mix_logic(scif, matches)
            shelf_scif, shelf_matches = self.calculate_displays_by_shelf_logic(scif, matches)

            scif = bin_bay_scif.append(bin_bin_scif)
            scif = scif.append(bin_shelf_scif)
            scif = scif.append(shelf_scif)
            scif = scif.append(bay_scif)
            scif.reset_index(drop=True, inplace=True)

            matches = bin_bay_matches.append(bin_bin_matches)
            matches = matches.append(bin_shelf_matches)
            matches = matches.append(shelf_matches)
            matches = matches.append(bay_matches)
            matches.reset_index(drop=True, inplace=True)
        return scif, matches

    def calculate_displays_separated_by_bays(self, scif, matches, max_display_id):
        bay_displays = self.displays_template[(self.displays_template[self.KPI_LOGIC] == 'Bin') &
                                              (self.displays_template[self.BAY_TO_SEPARATE] == 'No') &
                                              (self.displays_template[self.BIN_TO_SEPARATE] == 'No')] \
            [self.DISPLAY_NAME_TEMPL].unique()
        scif = scif[scif[ScifConsts.TEMPLATE_NAME].isin(bay_displays)]
        matches = matches[matches[ScifConsts.TEMPLATE_NAME].isin(bay_displays)]
        bay_scif = scif
        bay_matches = matches
        if not bay_matches.empty:
            bay_matches = matches.drop_duplicates(subset=[MatchesConsts.PRODUCT_FK, MatchesConsts.SCENE_FK],
                                                  keep='last')
            bay_matches[MatchesConsts.BAY_NUMBER] = 1
            bay_matches = self.construct_display_id(matches)
            bay_matches['display_id'] = bay_matches['display_id']+max_display_id
            bay_scif, bay_matches = self.calculate_product_length_on_display(bay_scif, bay_matches)
            max_display_id = bay_matches['display_id'].max()
        return bay_scif, bay_matches, max_display_id

    def place_posms_at_bay_minus_one_to_bays(self, matches):
        bay_number_minus_one = matches[matches[MatchesConsts.BAY_NUMBER] == -1]
        if not bay_number_minus_one.empty:
            filtered_matches = matches[~(matches[MatchesConsts.BAY_NUMBER] == -1)]
            bay_borders = filtered_matches.assign(start_x=filtered_matches['rect_x'],
                                                  end_x=filtered_matches['rect_x']).groupby([MatchesConsts.SCENE_FK,
                            MatchesConsts.BAY_NUMBER], as_index=False).agg({'start_x': np.min, 'end_x': np.max})
            bay_number_minus_one['new_bay_number'] = bay_number_minus_one.apply(self.find_bay_for_posm,
                                                                                args=(bay_borders,), axis=1)
            matches = matches.merge(bay_number_minus_one[[MatchesConsts.PROBE_MATCH_FK, 'new_bay_number']],
                                    on=MatchesConsts.PROBE_MATCH_FK, how='left')
            matches.loc[(matches[MatchesConsts.BAY_NUMBER] == -1), MatchesConsts.BAY_NUMBER] = \
                matches['new_bay_number']
            matches.drop('new_bay_number', axis=1, inplace=True)
            matches = matches[~(matches[MatchesConsts.BAY_NUMBER].isnull())]
        return matches

    def find_bay_for_posm(self, row, bay_borders):
        bay_df = bay_borders[(bay_borders[MatchesConsts.SCENE_FK] == row[MatchesConsts.SCENE_FK]) &
                             (bay_borders['start_x']<=row['rect_x']) & (bay_borders['end_x']>=row['rect_x'])]
        bay_number = bay_df[MatchesConsts.BAY_NUMBER].values[0] if not bay_df.empty else None
        return bay_number

    def construct_display_id(self, matches):
        scene_bay = matches[[MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER]].drop_duplicates()
        scene_bay = scene_bay.sort_values(by=[MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER])
        scene_bay = scene_bay.values.tolist()
        scene_bay = set(map(lambda x: tuple(x), scene_bay))
        display_dict = {}
        i = 0
        for value in scene_bay:
            display_dict[value] = i+1
            i += 1
        matches['display_id'] = matches.apply(self.assign_diplay_id, args=(display_dict, ), axis=1)
        return matches

    def assign_diplay_id(self, row, display_dict):
        x = (row[MatchesConsts.SCENE_FK], row[MatchesConsts.BAY_NUMBER])
        display_id = display_dict[x]
        return display_id

    def calculate_displays_by_shelf_logic(self, scif, matches):
        shelf_displays = self.displays_template[(self.displays_template[self.KPI_LOGIC] == 'Shelf')] \
            [self.DISPLAY_NAME_TEMPL].unique()
        scif = scif[scif[ScifConsts.TEMPLATE_NAME].isin(shelf_displays)]
        matches = matches[matches[ScifConsts.TEMPLATE_NAME].isin(shelf_displays)]
        return scif, matches

    def calculate_displays_by_mix_logic(self, scif, matches):
        mix_displays = self.displays_template[(self.displays_template[self.KPI_LOGIC] == 'Mix')] \
            [self.DISPLAY_NAME_TEMPL].unique()
        scif = scif[scif[ScifConsts.TEMPLATE_NAME].isin(mix_displays)]
        matches = matches[matches[ScifConsts.TEMPLATE_NAME].isin(mix_displays)]
        matches = matches.merge(self.shelf_len_mixed_shelves, on=[MatchesConsts.BAY_NUMBER, MatchesConsts.SCENE_FK],
                                how='left')
        mix_scif = scif
        mix_matches = matches
        if not mix_matches.empty:
            bottom_shelf = matches[MatchesConsts.SHELF_NUMBER].max()
            display_matches = mix_matches[mix_matches[MatchesConsts.SHELF_NUMBER] == bottom_shelf]
            shelf_matches = mix_matches[~(mix_matches[MatchesConsts.SHELF_NUMBER] == bottom_shelf)]
            bin_bin_scenes = self.scene_display[ScifConsts.SCENE_FK].unique()

            bin_bin_matches, bin_bay_matches = self.allocate_matches_to_logic(display_matches, bin_bin_scenes)
            if not bin_bin_matches.empty:
                bin_bin_matches = self.place_products_to_bays(bin_bin_matches, self.scene_display)
            display_matches = bin_bin_matches.append(bin_bay_matches)
            display_matches = self.calculate_product_length_in_matches_on_display(display_matches)

            mix_matches = shelf_matches.append(display_matches)
            mix_matches_agg = mix_matches.groupby([MatchesConsts.PRODUCT_FK, MatchesConsts.SCENE_FK]). \
                                            agg({'facings_matches': np.sum, MatchesConsts.WIDTH_MM_ADVANCE: np.sum})
            mix_scif = mix_scif.merge(mix_matches_agg, on=[MatchesConsts.PRODUCT_FK, MatchesConsts.SCENE_FK])
            mix_scif['facings'] = mix_scif['updated_facings'] = mix_scif['facings_matches']
            mix_scif[ScifConsts.GROSS_LEN_ADD_STACK] = mix_scif['updated_gross_length'] = mix_scif[
                MatchesConsts.WIDTH_MM_ADVANCE]

        return mix_scif, mix_matches

    def allocate_matches_to_logic(self, matches, bin_bin_scenes):
        bin_bin_matches = matches[matches[ScifConsts.SCENE_FK].isin(bin_bin_scenes)]
        bin_bay_matches = matches[(~matches[ScifConsts.SCENE_FK].isin(bin_bin_scenes))]
        return bin_bin_matches, bin_bay_matches

    def calculate_displays_by_bin_bin_logic(self, scif, matches, max_display_id = None):
        bin_bin_displays = self.displays_template[(self.displays_template[self.KPI_LOGIC] == 'Bin') &
                                                  (self.displays_template[self.BAY_TO_SEPARATE] == 'No') &
                                                  (self.displays_template[self.BIN_TO_SEPARATE] == 'Yes')] \
            [self.DISPLAY_NAME_TEMPL].unique()
        scif = scif[scif[ScifConsts.TEMPLATE_NAME].isin(bin_bin_displays)]
        matches = matches[matches[ScifConsts.TEMPLATE_NAME].isin(bin_bin_displays)]
        bin_bin_scif = scif
        bin_bin_matches = matches
        if not bin_bin_matches.empty:
            bin_bin_matches = self.place_products_to_bays(bin_bin_matches, self.scene_display, max_display_id)
            bin_bin_scif, bin_bin_matches = self.calculate_product_length_on_display(bin_bin_scif, bin_bin_matches)
        return bin_bin_scif, bin_bin_matches

    def calculate_product_length_in_matches_on_display(self, matches):
        matches = matches.drop_duplicates(
            subset=[MatchesConsts.PRODUCT_FK, MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER])
        matches_no_pos = matches[~(matches[MatchesConsts.STACKING_LAYER] == -2)]
        bay_sku = matches_no_pos.groupby([MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER],
                                  as_index=False).agg({'facings_matches': np.sum})
        bay_sku.rename(columns={'facings_matches': 'unique_skus'}, inplace=True)
        matches = matches.merge(bay_sku, on=[MatchesConsts.SCENE_FK, MatchesConsts.BAY_NUMBER], how='left')
        matches[MatchesConsts.WIDTH_MM_ADVANCE] = matches.apply(self.get_product_len, args=(matches,), axis=1)
        return matches

    def get_product_len(self, row, matches):
        if row[MatchesConsts.STACKING_LAYER] == -2:
            return 0

        # if Mix logic - then the length will depend of whether there are bins or bays in the bottom level
        if row[self.KPI_LOGIC] == 'Mix':
            # if there bins on bottom level
            if row[MatchesConsts.SCENE_FK] in self.scene_display[MatchesConsts.SCENE_FK].unique():
                number_of_bays = len(matches[matches[MatchesConsts.SCENE_FK] == row[MatchesConsts.SCENE_FK]] \
                                     [MatchesConsts.BAY_NUMBER].unique())
                if number_of_bays == 1:
                    leng = self.full_pallet_len / row['unique_skus']
                else:
                    leng = self.half_pallet_len / row['unique_skus']
                return leng

           #if there is just a regular shelf on bottom level
            else:
                leng = row['shelf_length'] / row['unique_skus']
                return leng
        # if logic is not Mixed, the produc len depends on the length of display and number of unique skus there
        else:
            leng = row[self.SHELF_LEN_DISPL] / row['unique_skus']
            return leng

    def calculate_product_length_on_display(self, scif, matches):
        matches = self.calculate_product_length_in_matches_on_display(matches)
        aggregated_matches = matches.groupby([MatchesConsts.PRODUCT_FK, MatchesConsts.SCENE_FK], as_index=False). \
            agg({'facings_matches': np.sum, MatchesConsts.WIDTH_MM_ADVANCE: np.sum})
        scif = scif.merge(aggregated_matches, on=[MatchesConsts.PRODUCT_FK, MatchesConsts.SCENE_FK], how='left')
        scif['facings'] = scif['updated_facings'] = scif['facings_matches']
        scif[ScifConsts.GROSS_LEN_ADD_STACK] = scif['updated_gross_length'] = scif[
            MatchesConsts.WIDTH_MM_ADVANCE]
        return scif, matches

    def place_products_to_bays(self, matches, scene_display, max_id=None):
        matches = matches.merge(scene_display, on=ScifConsts.SCENE_FK, how='left')
        matches.loc[(matches['rect_x'] >= matches['rect_x_start']) &
                    (matches['rect_x'] < matches['rect_x_end']), 'new_bay_number'] = matches['assigned_bay_number']
        matches = matches[~(matches['new_bay_number'].isnull())]
        matches['bay_number'] = matches['new_bay_number']
        matches = matches.drop('new_bay_number', axis=1)
        matches = matches.reset_index(drop=True)
        if max_id is not None:
            matches.drop('display_id', axis=1, inplace=True)
            matches = self.construct_display_id(matches)
            matches['display_id'] = matches['display_id'] + max_id
        return matches

    def assign_bays_to_bins(self):
        if not self.scene_display.empty:
            scene_display = self.scene_display[self.scene_display['display_name'] == 'Top Left Corner']
            scene_display = scene_display.sort_values(['scene_fk', 'rect_x'])
            scene_display = scene_display.assign(rect_x_end=scene_display.groupby('scene_fk').rect_x.shift(-1)). \
                fillna({'rect_x_end': np.inf})
            scene_display['assigned_bay_number'] = scene_display.groupby(['scene_fk'])['rect_x'].rank()
            scene_display.rename(columns={'rect_x': 'rect_x_start'}, inplace=True)
            scene_display.drop('bay_number', axis=1, inplace=True)
            self.scene_display = scene_display

    def calculate_displays_by_bin_bay_logic(self, scif, matches):
        bin_bay_displays = self.displays_template[(self.displays_template[self.KPI_LOGIC] == 'Bin') &
                                                  (self.displays_template[self.BAY_TO_SEPARATE] == 'Yes')] \
            [self.DISPLAY_NAME_TEMPL].unique()
        scif = scif[scif[ScifConsts.TEMPLATE_NAME].isin(bin_bay_displays)]
        matches = matches[matches[ScifConsts.TEMPLATE_NAME].isin(bin_bay_displays)]
        bin_bay_scif = scif
        bin_bay_matches = matches
        if not  bin_bay_matches.empty:
            bin_bay_matches = matches.drop_duplicates(subset=[MatchesConsts.PRODUCT_FK, MatchesConsts.BAY_NUMBER,
                                                              MatchesConsts.SCENE_FK], keep='last')
            bin_bay_scif, bin_bay_matches = self.calculate_product_length_on_display(bin_bay_scif, bin_bay_matches)
        return bin_bay_scif, bin_bay_matches

    def set_filtered_scif_and_matches_for_all_kpis_secondary(self, scif, matches):
        if self.do_exclusion_rules_apply_to_store('ALL'):
            excl_template_all_kpis = self.exclusion_template[self.exclusion_template['KPI'].str.upper() == 'ALL']
            if not excl_template_all_kpis.empty:
                template_filters = self.get_filters_dictionary(excl_template_all_kpis)
                scif, matches = self.filter_scif_and_matches_for_scene_and_product_filters(template_filters, scif,
                                                                                           matches)
                scif, matches = self.update_scif_and_matches_for_smart_attributes(scif, matches)
                scif, matches = self.add_sub_category_to_empty_and_other(scif, matches)
                scif, matches = self.recalculate_display_product_length(scif, matches)
                self.filtered_scif_secondary = scif
                self.filtered_matches_secondary = matches

    def get_initial_secondary_scif(self):
        scif = self.scif
        if not self.scif.empty:
            scif = self.scif[self.scif[ScifConsts.LOCATION_TYPE] == 'Secondary Shelf']
        return scif

    def get_initial_secondary_matches(self):
        matches = self.match_product_in_scene
        if not self.match_product_in_scene.empty:
            secondary_scenes = self.filtered_scif_secondary[ScifConsts.SCENE_FK].unique()
            matches = self.match_product_in_scene[self.match_product_in_scene[ScifConsts.SCENE_FK].isin(secondary_scenes)]
            if not matches.empty:
                matches = self.construct_display_id(matches)
        return matches

    def get_scene_to_store_area_map(self):
        query = PEPSICOUK_Queries.get_scene_store_area(self.session_uid)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def complete_scif_data(self):
        scene_store_area = self.get_scene_to_store_area_map()
        self.scif = self.scif.merge(scene_store_area, on=ScifConsts.SCENE_FK, how='left')
        self.match_product_in_scene = self.match_product_in_scene.merge(scene_store_area, on=ScifConsts.SCENE_FK,
                                                                        how='left')

    @staticmethod
    def split_and_strip(value):
        return map(lambda x: x.strip(), str(value).split(','))

    def get_store_policy_data_for_exclusion_template(self):
        store_policy = pd.read_excel(self.EXCLUSION_TEMPLATE_PATH, sheet_name='store_policy')
        store_policy = store_policy.fillna('ALL')
        return store_policy

    def get_kpi_result_values_df(self):
        query = PEPSICOUK_Queries.get_kpi_result_values()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_kpi_score_values_df(self):
        query = PEPSICOUK_Queries.get_kpi_score_values()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_store_data_by_store_id(self):
        store_id = self.store_id if self.store_id else self.session_info['store_fk'].values[0]
        query = PEPSICOUK_Queries.get_store_data_by_store_id(store_id)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_facings_scene_bay_shelf_product(self):
        self.filtered_matches['count'] = 1
        aggregate_df = self.filtered_matches.groupby(['scene_fk', 'bay_number', 'shelf_number',
                                                      'shelf_number_from_bottom', 'product_fk'],
                                                     as_index=False).agg({'count': np.sum})
        return aggregate_df

    def get_all_kpi_external_targets(self):
        query = PEPSICOUK_Queries.get_kpi_external_targets(self.visit_date)
        external_targets = pd.read_sql_query(query, self.rds_conn.db)
        return external_targets

    def get_custom_entity_data(self):
        query = PEPSICOUK_Queries.get_custom_entities_query()
        custom_entity_data = pd.read_sql_query(query, self.rds_conn.db)
        return custom_entity_data

    def get_on_display_products(self):
        probe_match_list = self.match_product_in_scene['probe_match_fk'].values.tolist()
        on_display_products = pd.DataFrame(columns=["probe_match_fk", "smart_attribute"])
        if probe_match_list:
            query = PEPSICOUK_Queries.on_display_products_query(probe_match_list)
            on_display_products = pd.read_sql_query(query, self.rds_conn.db)
        return on_display_products

    def get_exclusion_template_data(self):
        excl_templ = pd.read_excel(self.EXCLUSION_TEMPLATE_PATH)
        excl_templ = excl_templ.fillna('')
        return excl_templ

    def set_filtered_scif_and_matches_for_all_kpis(self, scif, matches):
        if self.do_exclusion_rules_apply_to_store('ALL'):
            excl_template_all_kpis = self.exclusion_template[self.exclusion_template['KPI'].str.upper() == 'ALL']
            if not excl_template_all_kpis.empty:
                template_filters = self.get_filters_dictionary(excl_template_all_kpis)
                scif, matches = self.filter_scif_and_matches_for_scene_and_product_filters(template_filters, scif, matches)
                scif, matches = self.update_scif_and_matches_for_smart_attributes(scif, matches)
                scif, matches = self.add_sub_category_to_empty_and_other(scif, matches)
                matches['facings_matches'] = 1
                self.filtered_scif = scif
                self.filtered_matches = matches

    def do_exclusion_rules_apply_to_store(self, kpi):
        exclusion_flag = True
        policy_template = self.store_policy_exclusion_template.copy()
        if kpi == 'ALL':
            policy_template['KPI'] = policy_template['KPI'].apply(lambda x: str(x).upper())
        relevant_policy = policy_template[policy_template['KPI'] == kpi]
        if not relevant_policy.empty:
            relevant_policy = relevant_policy.drop(columns='KPI')
            policy_columns = relevant_policy.columns.values.tolist()
            for column in policy_columns:
                store_att_value = self.store_info_dict.get(column)
                mask = relevant_policy.apply(self.get_masking_filter, args=(column, store_att_value), axis=1)
                relevant_policy = relevant_policy[mask]
            if relevant_policy.empty:
                exclusion_flag = False
        return exclusion_flag

    def get_masking_filter(self, row, column, store_att_value):
        if store_att_value in self.split_and_strip(row[column]) or row[column] == self.ALL:
            return True
        else:
            return False

    # def set_scif_and_matches_in_data_provider(self, scif, matches):
    #     self.data_provider._set_scene_item_facts(scif)
    #     self.data_provider._set_matches(matches)

    def get_filters_dictionary(self, excl_template_all_kpis):
        filters = {}
        for i, row in excl_template_all_kpis.iterrows():
            action = row['Action']
            if action == action:
                if action.upper() == 'INCLUDE':
                    filters.update({row['Type']: self.split_and_strip(row['Value'])})
                if action.upper() == 'EXCLUDE':
                    filters.update({row['Type']: (self.split_and_strip(row['Value']), 0)})
            else:
                Log.warning('Exclusion template: filter in row {} has no action will be omitted'.format(i+1))
        return filters

    def filter_scif_and_matches_for_scene_and_product_filters(self, template_filters, scif, matches):
        filters = self.get_filters_for_scif_and_matches(template_filters)
        scif = scif[self.toolbox.get_filter_condition(scif, **filters)]
        matches = matches[self.toolbox.get_filter_condition(matches, **filters)]
        return scif, matches

    def get_filters_for_scif_and_matches(self, template_filters):
        product_keys = filter(lambda x: x in self.all_products.columns.values.tolist(),
                              template_filters.keys())
        scene_keys = filter(lambda x: x in self.all_templates.columns.values.tolist(),
                            template_filters.keys())
        scene_keys.extend(filter(lambda x: x in self.store_areas.columns.values.tolist(),
                            template_filters.keys()))
        product_filters = {}
        scene_filters = {}
        for key in product_keys:
            product_filters.update({key: template_filters[key]})
        for key in scene_keys:
            scene_filters.update({key: template_filters[key]})

        filters_all = {}
        if product_filters:
            product_fks = self.get_product_fk_from_filters(product_filters)
            filters_all.update({'product_fk': product_fks})
        if scene_filters:
            scene_fks = self.get_scene_fk_from_filters(scene_filters)
            filters_all.update({'scene_fk': scene_fks})
        return filters_all

    def get_product_fk_from_filters(self, filters):
        all_products = self.all_products
        product_fk_list = all_products[self.toolbox.get_filter_condition(all_products, **filters)]
        product_fk_list = product_fk_list['product_fk'].unique().tolist()
        product_fk_list = product_fk_list if product_fk_list else [None]
        return product_fk_list

    def get_scene_fk_from_filters(self, filters):
        scif_data = self.scif
        scene_fk_list = scif_data[self.toolbox.get_filter_condition(scif_data, **filters)]
        scene_fk_list = scene_fk_list['scene_fk'].unique().tolist()
        scene_fk_list = scene_fk_list if scene_fk_list else [None]
        return scene_fk_list

    def update_scif_and_matches_for_smart_attributes(self, scif, matches):
        matches = self.filter_matches_for_products_with_smart_attributes(matches)
        aggregated_matches = self.aggregate_matches(matches)
        # remove relevant products from scif
        scif = self.update_scif_for_products_with_smart_attributes(scif, aggregated_matches)
        return scif, matches

    @staticmethod
    def update_scif_for_products_with_smart_attributes(scif, agg_matches):
        scif = scif.merge(agg_matches, on=['scene_fk', 'product_fk'], how='left')
        scif = scif[~scif['facings_matches'].isnull()]
        scif.rename(columns={'width_mm_advance': 'updated_gross_length', 'facings_matches': 'updated_facings'},
                    inplace=True)
        scif['facings'] = scif['updated_facings']
        scif['gross_len_add_stack'] = scif['updated_gross_length']
        return scif

    def filter_matches_for_products_with_smart_attributes(self, matches):
        matches = matches.merge(self.on_display_products, on='probe_match_fk', how='left')
        matches = matches[~(matches['smart_attribute'].isin([self.ADDITIONAL_DISPLAY, self.STOCK]))]
        return matches

    @staticmethod
    def aggregate_matches(matches):
        matches = matches[~(matches['bay_number'] == -1)]
        matches['facings_matches'] = 1
        aggregated_df = matches.groupby(['scene_fk', 'product_fk'], as_index=False).agg({'width_mm_advance': np.sum,
                                                                                         'facings_matches': np.sum})
        return aggregated_df

    def get_shelf_placement_kpi_targets_data(self):
        shelf_placement_targets = self.external_targets[self.external_targets['operation_type'] == self.SHELF_PLACEMENT]
        shelf_placement_targets = shelf_placement_targets.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk',
                                                                                  'key_json', 'data_json'])
        output_targets_df = pd.DataFrame(columns=shelf_placement_targets.columns.values.tolist())
        if not shelf_placement_targets.empty:
            shelf_number_df = self.unpack_external_targets_json_fields_to_df(shelf_placement_targets, field_name='key_json')
            shelves_to_include_df = self.unpack_external_targets_json_fields_to_df(shelf_placement_targets,
                                                                                   field_name='data_json')
            shelf_placement_targets = shelf_placement_targets.merge(shelf_number_df, on='pk', how='left')
            output_targets_df = shelf_placement_targets.merge(shelves_to_include_df, on='pk', how='left')
            kpi_data = self.kpi_static_data[['pk', 'type']]
            output_targets_df = output_targets_df.merge(kpi_data, left_on='kpi_level_2_fk', right_on='pk', how='left')
        return output_targets_df

    @staticmethod
    def unpack_external_targets_json_fields_to_df(input_df, field_name):
        data_list = []
        for i, row in input_df.iterrows():
            data_item = json.loads(row[field_name]) if row[field_name] else {}
            data_item.update({'pk': row.pk})
            data_list.append(data_item)
        output_df = pd.DataFrame(data_list)
        return output_df

    def unpack_all_external_targets(self):
        targets_df = self.external_targets.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk', 'key_json',
                                                                   'data_json'])
        output_targets = pd.DataFrame(columns=targets_df.columns.values.tolist())
        if not targets_df.empty:
            keys_df = self.unpack_external_targets_json_fields_to_df(targets_df, field_name='key_json')
            data_df = self.unpack_external_targets_json_fields_to_df(targets_df, field_name='data_json')
            targets_df = targets_df.merge(keys_df, on='pk', how='left')
            targets_df = targets_df.merge(data_df, on='pk', how='left')
            kpi_data = self.kpi_static_data[['pk', 'type']]
            kpi_data.rename(columns={'pk': 'kpi_level_2_fk'}, inplace=True)
            output_targets = targets_df.merge(kpi_data, on='kpi_level_2_fk', how='left')
        if output_targets.empty:
            Log.warning('KPI External Targets Results are empty')
        return output_targets

    def get_kpi_result_value_pk_by_value(self, value):
        pk = None
        try:
            pk = self.kpi_result_values[self.kpi_result_values['value'] == value]['pk'].values[0]
        except:
            Log.error('Value {} does not exist'.format(value))
        return pk

    def get_kpi_score_value_pk_by_value(self, value):
        pk = None
        try:
            pk = self.kpi_score_values[self.kpi_score_values['value'] == value]['pk'].values[0]
        except:
            Log.error('Value {} does not exist'.format(value))
        return pk

    def get_yes_no_score(self, score):
        value = 'NO' if not score else 'YES'
        custom_score = self.get_kpi_score_value_pk_by_value(value)
        return custom_score

    def get_yes_no_result(self, result):
        value = 'NO' if not result else 'YES'
        custom_score = self.get_kpi_result_value_pk_by_value(value)
        return custom_score

    def set_filtered_scif_and_matches_for_specific_kpi(self, scif, matches, kpi):
        try:
            kpi = int(float(kpi))
        except ValueError:
            kpi = kpi
        if isinstance(kpi, int):
            kpi = self.get_kpi_type_by_pk(kpi)
        if self.do_exclusion_rules_apply_to_store(kpi):
            excl_template_for_kpi = self.exclusion_template[self.exclusion_template['KPI'] == kpi]
            if not excl_template_for_kpi.empty:
                template_filters = self.get_filters_dictionary(excl_template_for_kpi)
                scif, matches = self.filter_scif_and_matches_for_scene_and_product_filters(template_filters, scif, matches)
        else:
            excl_template_for_kpi = self.exclusion_template[(self.exclusion_template['KPI'] == kpi) &
                                                            (self.exclusion_template['Ignore Store Policy'] == 1)]
            if not excl_template_for_kpi.empty:
                template_filters = self.get_filters_dictionary(excl_template_for_kpi)
                scif, matches = self.filter_scif_and_matches_for_scene_and_product_filters(template_filters, scif, matches)

        return scif, matches

    def get_kpi_type_by_pk(self, kpi_fk):
        try:
            kpi_fk = int(float(kpi_fk))
            return self.kpi_static_data[self.kpi_static_data['pk'] == kpi_fk]['type'].values[0]
        except IndexError:
            Log.info("Kpi name: {} is not equal to any kpi name in static table".format(kpi_fk))
            return None