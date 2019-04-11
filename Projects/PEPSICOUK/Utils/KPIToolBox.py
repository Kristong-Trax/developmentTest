
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
import numpy as np
import json
import collections

from KPIUtils_v2.DB.Common import Common as CommonV1
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Projects.PEPSICOUK.Utils.Fetcher import PEPSICOUK_Queries
from Projects.PEPSICOUK.Utils.CommonToolBox import PEPSICOUKCommonToolBox
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


class PEPSICOUKToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUSION_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                           'Inclusion_Exclusion_Template.xlsx')
    ADDITIONAL_DISPLAY = 'additional display'
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
    BRAND_FULL_BAY_KPIS = ['Brand Full Bay_90', 'Brand Full Bay']
    HERO_PREFIX = 'Hero SKU'
    ALL = 'ALL'
    HERO_SKU_OOS_SKU = 'Hero SKU OOS - SKU'
    HERO_SKU_OOS = 'Hero SKU OOS'
    HERO_SKU_AVAILABILITY = 'Hero SKU Availability'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        # self.common_v1 = CommonV1(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES] # initial matches
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS] # initial scif
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.toolbox = GENERALToolBox(self.data_provider)
        self.commontools = PEPSICOUKCommonToolBox(self.data_provider, self.rds_conn)

        self.custom_entities = self.commontools.custom_entities
        self.on_display_products = self.commontools.on_display_products
        self.exclusion_template = self.commontools.exclusion_template
        self.filtered_scif = self.commontools.filtered_scif
        self.filtered_matches = self.commontools.filtered_matches

        self.scene_bay_shelf_product = self.get_facings_scene_bay_shelf_product()
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.full_store_info = self.get_store_data_by_store_id()
        self.external_targets = self.commontools.external_targets
        self.assortment = Assortment(self.commontools.data_provider, self.output)
        # self.lvl3_ass_result = self.assortment.calculate_lvl3_assortment()
        self.lvl3_ass_result = self.get_lvl3_relevant_assortment_result()
        self.own_manuf_fk = self.all_products[self.all_products['manufacturer_name'] == self.PEPSICO]['manufacturer_fk'].values[0]

        self.scene_kpi_results = self.get_results_of_scene_level_kpis()
        self.kpi_results = pd.DataFrame(columns=['kpi_fk', 'numerator', 'denominator', 'result', 'score'])

#------------------init functions-----------------
    # def get_probe_group(self):
    #     query = PEPSICOUK_Queries.get_probe_group(self.session_uid)
    #     probe_group = pd.read_sql_query(query, self.rds_conn.db)
    #     return probe_group

    def get_results_of_scene_level_kpis(self):
        scene_kpi_results = pd.DataFrame()
        if not self.scene_info.empty:
            scene_kpi_results = self.ps_data.get_scene_results(self.scene_info['scene_fk'].drop_duplicates().values)
        return scene_kpi_results

    def get_store_data_by_store_id(self):
        store_id = self.store_id if self.store_id else self.session_info['store_fk'].values[0]
        query = PEPSICOUK_Queries.get_store_data_by_store_id(store_id)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_facings_scene_bay_shelf_product(self):
        self.filtered_matches['count'] = 1
        aggregate_df = self.filtered_matches.groupby(['scene_fk', 'bay_number', 'shelf_number', 'product_fk'],
                                                     as_index=False).agg({'count': np.sum})
        return aggregate_df

    # def set_scif_and_matches_in_data_provider(self, scif, matches):
    #     self.data_provider._set_scene_item_facts(scif)
    #     self.data_provider._set_matches(matches)

#------------------utility functions-------------
    @staticmethod
    def split_and_strip(value):
        return map(lambda x: x.strip(), str(value).split(','))

    def add_kpi_result_to_kpi_results_df(self, result_list):
        self.kpi_results.loc[len(self.kpi_results)] = result_list

#------------------main project calculations------

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates and writes to DB the KPI results.
        """
        self.calculate_external_kpis()
        self.calculate_internal_kpis()

    def calculate_external_kpis(self):
        self.calculate_assortment() # uses filtered scif
        self.calculate_sos_vs_target_kpis() # uses filtered scif
        self.calculate_linear_brand_vs_brand_index() # uses filtered scif
        self.calculate_shelf_placement_hero_skus() # uses both initial and filtered scif / matches
        self.calculate_brand_full_bay()  # uses filtered matches
        self.calculate_hero_sku_information_kpis() # uses filtered matches
        self.calculate_hero_sku_stacking_by_sequence_number()

    def calculate_brand_full_bay(self):
        brand_full_bay_kpi_fks = [self.common.get_kpi_fk_by_kpi_type(kpi) for kpi in self.BRAND_FULL_BAY_KPIS]
        external_kpi_targets = self.commontools.all_targets_unpacked[self.commontools.all_targets_unpacked['kpi_level_2_fk'].isin(brand_full_bay_kpi_fks)]
        external_kpi_targets = external_kpi_targets.reset_index(drop=True)
        if not external_kpi_targets.empty:
            external_kpi_targets['group_fk'] = external_kpi_targets['Group Name'].apply(lambda x:
                                                                self.custom_entities[self.custom_entities['name'] == x]['pk'].values[0])
            filtered_matches = self.filtered_matches[~(self.filtered_matches['bay_number'] == -1)]
            if not filtered_matches.empty:
                scene_bay_product = filtered_matches.groupby(['scene_fk', 'bay_number', 'product_fk'],
                                                             as_index=False).agg({'count': np.sum})
                scene_bay_product = scene_bay_product.merge(self.all_products, on='product_fk', how='left')
                scene_bay = scene_bay_product.groupby(['scene_fk', 'bay_number'], as_index=False).agg({'count':np.sum})
                scene_bay.rename(columns={'count': 'total_facings'}, inplace=True)
                for i, row in external_kpi_targets.iterrows():
                    filters = self.get_full_bay_and_positional_filters(row)
                    brand_relevant_df = scene_bay_product[self.toolbox.get_filter_condition(scene_bay_product, **filters)]
                    result_df = brand_relevant_df.groupby(['scene_fk', 'bay_number'], as_index=False).agg({'count':np.sum})
                    result_df = result_df.merge(scene_bay, on=['scene_fk', 'bay_number'], how='left')
                    result_df['ratio'] = result_df['count'] / result_df['total_facings']
                    result_100 = len(result_df[result_df['ratio'] >= 1])
                    result_90 = len(result_df[result_df['ratio'] >= 0.9])
                    self.common.write_to_db_result(fk=row['kpi_level_2_fk'], numerator_id=row['group_fk'], score=result_100)
                    kpi_90_fk = self.common.get_kpi_fk_by_kpi_type('Brand Full Bay_90')
                    self.common.write_to_db_result(fk=kpi_90_fk,  numerator_id=row['group_fk'], score=result_90)

                    self.add_kpi_result_to_kpi_results_df([row['kpi_level_2_fk'], row['group_fk'], None, None, result_100])
                    self.add_kpi_result_to_kpi_results_df([kpi_90_fk, row['group_fk'], None, None, result_90])

    @staticmethod
    def get_full_bay_and_positional_filters(parameters): # get a function from ccbza
        filters = {parameters['Parameter 1']: parameters['Value 1']}
        if parameters['Parameter 2']:
            filters.update({parameters['Parameter 2']: parameters['Value 2']})
        if parameters['Parameter 3']:
            filters.update({parameters['Parameter 3']: parameters['Value 3']})
        return filters

    def get_relevant_full_bay_kpi_targets(self):
        brand_full_bay_kpi_fks = [self.common.get_kpi_fk_by_kpi_type(kpi) for kpi in self.BRAND_FULL_BAY_KPIS]
        full_bay_targets = self.external_targets[self.external_targets['kpi_level_2_fk'].isin(brand_full_bay_kpi_fks)]
        full_bay_targets = full_bay_targets.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk',
                                                                    'key_json', 'data_json'])
        output_targets_df = pd.DataFrame(columns=full_bay_targets.columns.values.tolist())
        if not full_bay_targets.empty:
            groups_df = self.commontools.unpack_external_targets_json_fields_to_df(full_bay_targets, field_name='key_json')
            full_bay_targets = full_bay_targets.merge(groups_df, on='pk', how='left')
            parameters_df = self.commontools.unpack_external_targets_json_fields_to_df(full_bay_targets, field_name='data_json')
            output_targets_df = full_bay_targets.merge(parameters_df, on='pk', how='left')
        return output_targets_df

    def calculate_hero_sku_information_kpis(self):
        if not self.lvl3_ass_result.empty:
            hero_sku_list = self.lvl3_ass_result[self.lvl3_ass_result['in_store'] == 1]['product_fk'].values.tolist()
            # hero_sku_list = self.filtered_scif['product_fk'].unique().tolist() # comment after
            # stacking_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.HERO_SKU_STACKING)
            price_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.HERO_SKU_PRICE)
            promo_price_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.HERO_SKU_PROMO_PRICE)
            for sku in hero_sku_list:
                # self.calculate_hero_sku_stacking_width(sku, stacking_kpi_fk)
                # self.calculate_hero_sku_stacking_layer_more_than_one(sku, stacking_kpi_fk)
                self.calculate_hero_sku_price(sku, price_kpi_fk)
                self.calculate_hero_sku_promo_price(sku, promo_price_kpi_fk)

    def calculate_hero_sku_promo_price(self, sku, kpi_fk):
        price = 0
        prices_df = self.filtered_matches[(~(self.filtered_matches['promotion_price'].isnull())) &
                                          (self.filtered_matches['product_fk'] == sku)]
        if not prices_df.empty:
            price = 1
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=sku, result=price)
        self.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, price, None])

    def calculate_hero_sku_price(self, sku, kpi_fk):
        # what should we write if there is no price at all?
        price = -1
        prices_df = self.filtered_matches[(~(self.filtered_matches['price'].isnull())) &
                                          (self.filtered_matches['product_fk'] == sku)]
        if not prices_df.empty:
            price = prices_df['price'].max()
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=sku, result=price)
        self.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, price, None])

    def calculate_hero_sku_stacking_by_sequence_number(self):
        if not self.lvl3_ass_result.empty:
            hero_list = self.lvl3_ass_result[self.lvl3_ass_result['in_store'] == 1]['product_fk'].unique().tolist()
            # hero_list = self.filtered_scif['product_fk'].unique().tolist()
            relevant_matches = self.filtered_matches[self.filtered_matches['product_fk'].isin(hero_list)]
            relevant_matches = relevant_matches.reset_index(drop=True)
            relevant_matches['facing_sequence_number'] = relevant_matches['facing_sequence_number'].astype(str)
            relevant_matches['all_sequences'] = relevant_matches.groupby(['scene_fk', 'bay_number', 'shelf_number', 'product_fk']) \
                                                    ['facing_sequence_number'].apply(lambda x: (x + ',').cumsum().str.strip())
            grouped_matches = relevant_matches.drop_duplicates(subset=['scene_fk', 'bay_number', 'shelf_number', 'product_fk'],
                                                               keep='last')
            grouped_matches['is_stack'] = grouped_matches.apply(self.get_stack_data, axis=1)
            stacking_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.HERO_SKU_STACKING)
            for sku in hero_list:
                stack_info = grouped_matches[grouped_matches['product_fk'] == sku]['is_stack'].values.tolist()
                score = 0
                if any(stack_info):
                    score = 1
                self.common.write_to_db_result(fk=stacking_kpi_fk, numerator_id=sku, score=score, result=score)

                self.add_kpi_result_to_kpi_results_df([stacking_kpi_fk, sku, None, score, score])

    @staticmethod
    def get_stack_data(row):
        is_stack = False
        sequences_list = row['all_sequences'][0:-1].split(',')
        count_sequences = collections.Counter(sequences_list)
        repeating_items = [c > 1 for c in count_sequences.values()]
        if repeating_items:
            if any(repeating_items):
                is_stack = True
        return is_stack

    def calculate_hero_sku_stacking_layer_more_than_one(self, sku, kpi_fk):
        score = 0
        product_df = self.filtered_matches[(self.filtered_matches['product_fk'] == sku) &
                                           (self.filtered_matches['stacking_layer'] > 1)]
        if not product_df.empty:
            score = 1
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=sku, score=score, result=score)
        self.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, score, score])

    def calculate_hero_sku_stacking_width(self, sku, kpi_fk):
        score = 0
        relevant_matches = self.filtered_matches[self.filtered_matches['product_fk'] == sku]
        # get only those locations where sku appears
        entities = relevant_matches.drop_duplicates(subset=['scene_fk', 'bay_number', 'shelf_number'])
        for i, entity in entities.iterrows():
            result_df = self.filtered_matches[(self.filtered_matches['product_fk'] == sku) &
                                              (self.filtered_matches['scene_fk'] == entity['scene_fk']) &
                                              (self.filtered_matches['bay_number'] == entity['bay_number']) &
                                              (self.filtered_matches['shelf_number'] == entity['shelf_number'])]
            stacking_layers = result_df['stacking_layer'].unique().tolist()
            if len(stacking_layers) > 1:
                result_df['x_mm_left'] = result_df['x_mm'] - result_df['width_mm_advance'] / 2
                result_df['x_mm_right'] = result_df['x_mm'] + result_df['width_mm_advance'] / 2
                for i, row in result_df.iterrows():
                    upper_stacking_df = result_df[result_df['stacking_layer'] > row['stacking_layer']]
                    upper_stacking_df = upper_stacking_df[~((upper_stacking_df['x_mm_right'] <= row['x_mm_left']) |
                                                            (upper_stacking_df['x_mm_left'] >= row['x_mm_right']))]
                    if not upper_stacking_df.empty:
                        score = 1
                        break

        self.common.write_to_db_result(fk=kpi_fk, numerator_id=sku, score=score, result=score)
        self.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, score, score])

    def calculate_shelf_placement_hero_skus(self):
        if not self.lvl3_ass_result.empty:
        # if not self.filtered_scif.empty:
            external_targets = self.commontools.all_targets_unpacked
            shelf_placmnt_targets = external_targets[external_targets['operation_type'] == self.SHELF_PLACEMENT]
            kpi_fks = shelf_placmnt_targets['kpi_level_2_fk'].unique().tolist()
            scene_placement_results = self.scene_kpi_results[self.scene_kpi_results['kpi_level_2_fk'].isin(kpi_fks)]
            if not scene_placement_results.empty:
                hero_results = self.get_hero_results_df(scene_placement_results)
                if not hero_results.empty:
                    kpis_df = self.kpi_static_data[['pk', 'type']]
                    kpis_df.rename(columns={'pk': 'kpi_level_2_fk'}, inplace=True)
                    hero_results = hero_results.merge(kpis_df, on='kpi_level_2_fk', how='left')
                    # hero_results['parent_type'] = hero_results['KPI Parent'].apply(self.get_kpi_type_by_pk)
                    hero_results['parent_type'] = hero_results['KPI Parent']
                    hero_results['type'] = hero_results['type'].apply(lambda x: '{} {}'.format(self.HERO_PREFIX, x))
                    hero_results['parent_type'] = hero_results['parent_type'].apply(lambda x: '{} {}'.format(self.HERO_PREFIX, x))
                    hero_results['kpi_level_2_fk'] = hero_results['type'].apply(self.common.get_kpi_fk_by_kpi_type)
                    hero_results['KPI Parent'] = hero_results['parent_type'].apply(self.common.get_kpi_fk_by_kpi_type)
                    hero_results['identifier_parent'] = hero_results.apply(self.construct_hero_identifier_dict, axis=1)

                    for i, row in hero_results.iterrows():
                        self.common.write_to_db_result(fk=row['kpi_level_2_fk'], numerator_id=row['numerator_id'],
                                                       denominator_id=row['numerator_id'], denominator_result=row['denominator_result'],
                                                       numerator_result=row['numerator_result'], result=row['ratio'],
                                                       score=row['ratio'], identifier_parent=row['identifier_parent'],
                                                       should_enter=True)
                        self.add_kpi_result_to_kpi_results_df([row.kpi_level_2_fk, row.numerator_id, row['numerator_id'], row['ratio'],
                                                               row['ratio']])
                    hero_parent_results = hero_results.groupby(['numerator_id', 'KPI Parent'], as_index=False).agg({'ratio': np.sum})
                    hero_parent_results['identifier_parent'] = hero_parent_results.apply(self.construct_hero_identifier_dict, axis=1)

                    top_sku_parent = self.common.get_kpi_fk_by_kpi_type(self.HERO_PLACEMENT)
                    top_parent_identifier_par = self.common.get_dictionary(kpi_fk=top_sku_parent)
                    for i, row in hero_parent_results.iterrows():
                        self.common.write_to_db_result(fk=row['KPI Parent'], numerator_id=row['numerator_id'], result=row['ratio'],
                                                       score=row['ratio'], identifier_result=row['identifier_parent'],
                                                       identifier_parent=top_parent_identifier_par, denominator_id=self.store_id,
                                                       should_enter=True)
                        self.add_kpi_result_to_kpi_results_df([row['KPI Parent'], row.numerator_id, self.store_id, row['ratio'],
                                                                row['ratio']])
                    self.common.write_to_db_result(fk=top_sku_parent, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                                   result=len(hero_parent_results), score=len(hero_parent_results),
                                                   identifier_result=top_parent_identifier_par, should_enter=True)
                    self.add_kpi_result_to_kpi_results_df([top_sku_parent, self.own_manuf_fk, self.store_id, len(hero_parent_results),
                                                           len(hero_parent_results)])

    @staticmethod
    def construct_hero_identifier_dict(row):
        id_dict = {'kpi_fk': int(float(row['KPI Parent'])), 'sku': row['numerator_id']}
        return id_dict

    def get_hero_results_df(self, scene_placement_results):
        kpi_results = scene_placement_results.groupby(['kpi_level_2_fk', 'numerator_id'], as_index=False).agg(
            {'numerator_result': np.sum})
        products_df = scene_placement_results.groupby(['numerator_id'], as_index=False).agg(
            {'numerator_result': np.sum})
        products_df.rename(columns={'numerator_result': 'denominator_result'}, inplace=True)
        kpi_results = kpi_results.merge(products_df, on='numerator_id', how='left')
        hero_skus = self.lvl3_ass_result[self.lvl3_ass_result['in_store'] == 1]['product_fk'].values.tolist()
        # hero_skus = self.filtered_scif['product_fk'].unique().tolist() # uncomment after
        hero_results = kpi_results[kpi_results['numerator_id'].isin(hero_skus)]
        kpi_parent = self.commontools.all_targets_unpacked.drop_duplicates(subset=['kpi_level_2_fk', 'KPI Parent'])[['kpi_level_2_fk', 'KPI Parent']]
        hero_results = hero_results.merge(kpi_parent, on='kpi_level_2_fk')
        # hero_results['ratio'] = hero_results.apply(self.get_sku_ratio, axis=1)
        hero_results['ratio'] = hero_results['numerator_result'] / hero_results['denominator_result']
        return hero_results

    def get_kpi_type_by_pk(self, kpi_fk):
        # assert isinstance(kpi_fk, (unicode, basestring)), "name is not a string: %r" % kpi_fk
        try:
            kpi_fk = int(float(kpi_fk))
            return self.kpi_static_data[self.kpi_static_data['pk'] == kpi_fk]['type'].values[0]
        except IndexError:
            Log.info("Kpi name: {} is not equal to any kpi name in static table".format(kpi_fk))
            return None

    def get_kpi_results_df(self, relevant_matches, kpi_targets_df):
        total_products_facings = relevant_matches.groupby(['product_fk'], as_index=False).agg({'count': np.sum})
        total_products_facings.rename(columns={'count': 'total_facings'})
        result_df = relevant_matches.groupby(['product_fk', 'type'], as_index=False).agg({'count':np.sum})
        result_df.merge(total_products_facings, on='product_fk', how='left')

        kpis_df = kpi_targets_df.drop_duplicates(subset=['kpi_level_2_fk', 'type', 'KPI Parent'])
        result_df = result_df.merge(kpis_df, on='type', how='left')
        result_df['identifier_parent'] = result_df['KPI Parent'].apply(lambda x:
                                                                       self.common.get_dictionary(
                                                                       kpi_fk=int(float(x))))
        result_df['ratio'] = result_df.apply(self.get_sku_ratio, axis=1)
        hero_results = result_df[result_df['product_fk'].isin(self.lvl3_ass_result['product_fk'].values.tolist())]
        return hero_results

    @staticmethod
    def get_sku_ratio(row):
        ratio = row['count'] / row['total_facings']
        return ratio

    def filter_out_irrelevant_matches(self, target_kpis_df):
        relevant_matches = self.scene_bay_shelf_product[~(self.scene_bay_shelf_product['bay_number'] == -1)]
        for i, row in target_kpis_df.iterrows():
            all_shelves = map(lambda x: float(x), row['shelves_all_placements'].split(','))
            rows_to_remove = relevant_matches[(relevant_matches['scene_fk'] == row['scene_fk']) &
                                              (relevant_matches['bay_number'] == row['bay_number']) &
                                              (~(relevant_matches['shelf_number'].isin(all_shelves)))].index
            relevant_matches.drop(rows_to_remove, inplace=True)
        relevant_matches['position'] = ''
        return relevant_matches

    def get_scene_bay_max_shelves(self, shelf_placement_targets):
        scene_bay_max_shelves = self.match_product_in_scene.groupby(['scene_fk', 'bay_number'],
                                                                    as_index=False).agg({'shelf_number': np.max})
        scene_bay_max_shelves.rename({'shelf_number': 'shelves_in_bay'})
        scene_bay_max_shelves = scene_bay_max_shelves.merge(shelf_placement_targets, left_on='shelves_in_bay',
                                                            right_on='No of Shelves in Fixture (per bay) (key)')
        scene_bay_max_shelves = self.complete_missing_target_shelves(scene_bay_max_shelves)
        scene_bay_max_shelves['shelves_all_placements'] = scene_bay_max_shelves.groupby(['scene_fk', 'bay_number']) \
                                            ['Shelves From Bottom To Include (data)'].apply(lambda x: ','.join(str(x))) # need to debug
        relevant_scenes = self.filtered_matches['scene_fk'].unique().tolist()
        scene_bay_max_shelves = scene_bay_max_shelves[(scene_bay_max_shelves['scene_fk'].isin(relevant_scenes)) &
                                                      (~(scene_bay_max_shelves['bay_number']==-1))]
        final_df = pd.DataFrame(columns=scene_bay_max_shelves.columns.value.tolist())
        for i, row in scene_bay_max_shelves.iterrows():
            relevant_bays = self.filtered_matches[self.filtered_matches['scene_fk']==row['scene_fk']]['bay_number'].values.tolist()
            if row['bay_number'] in relevant_bays:
                final_df.append(row)
        return final_df

    def complete_missing_target_shelves(self, scene_bay_df):
        for i, row in scene_bay_df.iterrows():
            if row['shelves_in_bay'] > 7:
                scene_bay_df.loc[(scene_bay_df['scene_fk'] == row['scene_fk']) &
                                 (scene_bay_df['bay_number'] == row['bay_number']) &
                                 (scene_bay_df['type'] == self.HERO_SKU_PLACEMENT_TOP),
                                 'Shelves From Bottom To Include (data)'] = row['shelves_in_bay']
                scene_bay_df.loc[(scene_bay_df['scene_fk'] == row['scene_fk']) &
                                 (scene_bay_df['bay_number'] == row['bay_number']) &
                                 (scene_bay_df['type'] == self.HERO_SKU_PLACEMENT_TOP),
                                 'No of Shelves in Fixture (per bay) (key)'] = row['shelves_in_bay']
        scene_bay_df = scene_bay_df[~scene_bay_df['Shelves From Bottom To Include (data)'].isnull()]
        return scene_bay_df

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

    def calculate_linear_brand_vs_brand_index(self):
        # index_targets = self.get_relevant_sos_index_kpi_targets_data()
        index_targets = self.get_relevant_sos_vs_target_kpi_targets(brand_vs_brand=True)
        index_targets['numerator_id'] = index_targets.apply(self.retrieve_relevant_item_pks, axis=1,
                                                            args=('numerator_type', 'numerator_value'))
        index_targets['denominator_id'] = index_targets.apply(self.retrieve_relevant_item_pks, axis=1,
                                                              args=('denominator_type', 'denominator_value'))
        index_targets['identifier_parent'] = index_targets['KPI Parent'].apply(lambda x:
                                                                           self.common.get_dictionary(
                                                                               kpi_fk=int(float(x))))
        for i, row in index_targets.iterrows():
            general_filters = {row['additional_filter_type_1']: row['additional_filter_value_1']}
            numerator_sos_filters = {row['numerator_type']: row['numerator_value']}
            num_num_linear, num_denom_linear = self.calculate_sos(numerator_sos_filters, **general_filters)
            numerator_sos = num_num_linear/num_denom_linear if num_denom_linear else 0

            denominator_sos_filters = {row['denominator_type']: row['denominator_value']}
            denom_num_linear, denom_denom_linear = self.calculate_sos(denominator_sos_filters, **general_filters)
            denominator_sos = denom_num_linear/denom_denom_linear if denom_denom_linear else 0

            index = numerator_sos / denominator_sos if denominator_sos else 0
            self.common.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                           numerator_result=num_num_linear, denominator_id=row.denominator_id,
                                           denominator_result=denom_num_linear, result=index, score=index,
                                           identifier_parent=row.identifier_parent, should_enter=True)
            self.add_kpi_result_to_kpi_results_df([row.kpi_level_2_fk, row.numerator_id, row.denominator_id, index,
                                                   index])

        parent_kpis_df = index_targets.drop_duplicates(subset=['KPI Parent'])
        parent_kpis_df.rename(columns={'identifier_parent': 'identifier_result'}, inplace=True)
        for i, row in parent_kpis_df.iterrows():
            self.common.write_to_db_result(fk=row['KPI Parent'], numerator_id=self.own_manuf_fk,
                                           denominator_id=self.store_id, score=1,
                                           identifier_result=row.identifier_result, should_enter=True)
            self.add_kpi_result_to_kpi_results_df([row['KPI Parent'], self.own_manuf_fk, self.store_id, None,
                                                   1])

    def get_lvl3_relevant_assortment_result(self):
        assortment_result = self.assortment.get_lvl3_relevant_ass()
        if assortment_result.empty:
            return assortment_result
        products_in_session = self.filtered_scif.loc[self.filtered_scif['facings'] > 0]['product_fk'].values
        assortment_result.loc[assortment_result['product_fk'].isin(products_in_session), 'in_store'] = 1
        return assortment_result

    def calculate_assortment(self):
        oos_sku_fk = self.common.get_kpi_fk_by_kpi_type(self.HERO_SKU_OOS_SKU)
        oos_fk = self.common.get_kpi_fk_by_kpi_type(self.HERO_SKU_OOS)
        distribution_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.HERO_SKU_AVAILABILITY)
        identifier_parent = self.common.get_dictionary(kpi_fk=distribution_kpi_fk)
        oos_identifier_parent = self.common.get_dictionary(kpi_fk=oos_fk)
        for i, result in self.lvl3_ass_result.iterrows():
            score = result.in_store * 100
            custom_res = self.commontools.get_yes_no_result(score)
            self.common.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                           numerator_result=result.in_store, result=custom_res,
                                           denominator_id=self.store_id, should_enter=True,
                                           denominator_result=1, score=score, identifier_parent=identifier_parent)
            self.add_kpi_result_to_kpi_results_df([result['kpi_fk_lvl3'], result['product_fk'], self.store_id, custom_res,
                                                   score])
            if score == 0:
                # OOS
                self.common.write_to_db_result(fk=oos_sku_fk,
                                               numerator_id=result.product_fk, numerator_result=score,
                                               result=score, denominator_id=self.store_id,
                                               denominator_result=1, score=score,
                                               identifier_parent=oos_identifier_parent, should_enter=True)

        if not self.lvl3_ass_result.empty:
            lvl2_result = self.assortment.calculate_lvl2_assortment(self.lvl3_ass_result)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                if result.target and not np.math.isnan(result.target):
                    if result.group_target_date <= self.visit_date:
                        denominator_res = result.target
                res = np.divide(float(result.passes), float(denominator_res)) * 100
                score = 100 if res >= 100 else 0
                self.common.write_to_db_result(fk=result.kpi_fk_lvl2, numerator_id=self.own_manuf_fk,
                                               numerator_result=result.passes, result=res,
                                               denominator_id=self.store_id, denominator_result=denominator_res, score=score,
                                               identifier_result=identifier_parent, should_enter=True)
                self.add_kpi_result_to_kpi_results_df([result.kpi_fk_lvl2, self.own_manuf_fk, self.store_id, res, score])

                self.common.write_to_db_result(fk=oos_fk, numerator_id=self.own_manuf_fk,
                                               numerator_result=denominator_res - result.passes,
                                               denominator_id=self.store_id, denominator_result=denominator_res,
                                               result=1 - res, score=(1 - res), identifier_result=oos_identifier_parent,
                                               should_enter=True)

    def calculate_sos_vs_target_kpis(self):
        sos_targets = self.get_relevant_sos_vs_target_kpi_targets()
        if not sos_targets.empty:
            # all_products_columns = self.all_products.columns.values.tolist()
            sos_targets['numerator_id'] = sos_targets.apply(self.retrieve_relevant_item_pks, axis=1,
                                                            args=('numerator_type', 'numerator_value'))
            sos_targets['denominator_id'] = sos_targets.apply(self.retrieve_relevant_item_pks, axis=1,
                                                              args=('denominator_type', 'denominator_value'))
            sos_targets['identifier_parent'] = sos_targets['KPI Parent'].apply(lambda x:
                                                                self.common.get_dictionary(kpi_fk=int(float(x))))
            for i, row in sos_targets.iterrows():
                general_filters = {row['denominator_type']: row['denominator_value']}
                sos_filters = {row['numerator_type']: row['numerator_value']}
                numerator_linear, denominator_linear = self.calculate_sos(sos_filters, **general_filters)
                result = numerator_linear/denominator_linear if denominator_linear != 0 else 0
                score = result/row['Target'] if row['Target'] else 0 # todo: what should we have in else case???
                self.common.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                               numerator_result=numerator_linear, denominator_id=row.denominator_id,
                                               denominator_result=denominator_linear, result=result, score=score,
                                               target=row['Target'], identifier_parent=row.identifier_parent,
                                               should_enter=True)
                self.add_kpi_result_to_kpi_results_df([row.kpi_level_2_fk, row.numerator_id, row.denominator_id, result,
                                                       score])

            sos_targets['count'] = 1
            parent_kpis_df = sos_targets.groupby(['KPI Parent'], as_index=False).agg({'count': np.sum})
            parent_kpis_df['identifier_parent'] = parent_kpis_df['KPI Parent'].apply(lambda x:
                                                                self.common.get_dictionary(kpi_fk=int(float(x))))
            for i, row in parent_kpis_df.iterrows():
                self.common.write_to_db_result(fk=row['KPI Parent'], score=row['count'], should_enter=True,
                                               numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                               identifier_result=row['identifier_parent'])
                self.add_kpi_result_to_kpi_results_df([row['KPI Parent'], self.own_manuf_fk, self.store_id, None,
                                                       row['count']])

    def get_relevant_sos_vs_target_kpi_targets(self, brand_vs_brand=False):
        sos_vs_target_kpis = self.external_targets[self.external_targets['operation_type'] == self.SOS_VS_TARGET]
        sos_vs_target_kpis = sos_vs_target_kpis.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk',
                                                                        'key_json', 'data_json'])
        relevant_targets_df = pd.DataFrame(columns=sos_vs_target_kpis.columns.values.tolist())
        if not sos_vs_target_kpis.empty:
            policies_df = self.commontools.unpack_external_targets_json_fields_to_df(sos_vs_target_kpis,
                                                                                     field_name='key_json')
            policy_columns = policies_df.columns.values.tolist()
            del policy_columns[policy_columns.index('pk')]
            store_dict = self.full_store_info.to_dict('records')[0]
            for column in policy_columns:
                store_att_value = store_dict.get(column)
                policies_df = policies_df[policies_df[column].isin([store_att_value, self.ALL])]
            kpi_targets_pks = policies_df['pk'].values.tolist()
            relevant_targets_df = sos_vs_target_kpis[sos_vs_target_kpis['pk'].isin(kpi_targets_pks)]
            # relevant_targets_df = relevant_targets_df.merge(policies_df, on='pk', how='left')
            data_json_df = self.commontools.unpack_external_targets_json_fields_to_df(relevant_targets_df, 'data_json')
            relevant_targets_df = relevant_targets_df.merge(data_json_df, on='pk', how='left')

            kpi_data = self.kpi_static_data[['pk', 'type']].drop_duplicates()
            kpi_data.rename(columns={'pk': 'kpi_level_2_fk'}, inplace=True)
            relevant_targets_df = relevant_targets_df.merge(kpi_data, left_on='kpi_level_2_fk', right_on='kpi_level_2_fk', how='left')
            linear_sos_fk = self.common.get_kpi_fk_by_kpi_type(self.LINEAR_SOS_INDEX)
            if brand_vs_brand:
                relevant_targets_df = relevant_targets_df[relevant_targets_df['KPI Parent'] == linear_sos_fk]
            else:
                relevant_targets_df = relevant_targets_df[~(relevant_targets_df['KPI Parent'] == linear_sos_fk)]
        return relevant_targets_df

    def retrieve_relevant_item_pks(self, row, type_field_name, value_field_name):
        try:
            if row[type_field_name].endswith("_fk"):
                item_id = row[value_field_name]
            else:
                # print row[type_field_name], ' :', row[value_field_name]
                item_id = self.custom_entities[self.custom_entities['name'] == row[value_field_name]]['pk'].values[0]
        except KeyError as e:
            Log.error('No id found for field {}. Error: {}'.format(row[type_field_name], e))
            item_id = None
        return item_id

    @staticmethod
    def unpack_external_targets_json_fields_to_df(input_df, field_name):
        data_list = []
        for row in input_df.iterrows():
            data_item = json.loads(row[field_name])
            data_item.update({'pk': row.pk})
            data_list.append(data_item)
        output_df = pd.DataFrame(data_list)
        return output_df

    def calculate_sos(self, sos_filters, **general_filters):
        numerator_linear = self.calculate_share_space(**dict(sos_filters, **general_filters))
        denominator_linear = self.calculate_share_space(**general_filters)
        return float(numerator_linear), float(denominator_linear)

    def calculate_share_space(self, **filters):
        filtered_scif = self.filtered_scif[self.toolbox.get_filter_condition(self.filtered_scif, **filters)]
        space_length = filtered_scif['updated_gross_length'].sum()
        return space_length

    def calculate_internal_kpis(self):
        pass
