
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import json
import numpy as np
# import os
import copy

from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from KPIUtils_v2.Utils.Parsers.ParseInputKPI import filter_df
from Projects.MARSUAE.Utils.Fetcher import MARSUAE_Queries
from Projects.MARSUAE.Utils.Nodes import Node
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts


__author__ = 'natalyak'


class MARSUAEToolBox:
    CATEGORY_LEVEL = 'category_level'
    ATOMIC_LEVEL = 'atomic_level'
    AVAILABILITY = 'Availability'
    LINEAR_SOS = 'Linear SOS'
    POI = 'POI'
    PENETRATION = 'Penetration'
    BLOCK = 'Block'
    CHECKOUTS_COUNT = 'Checkouts Count'
    DISPLAYS_COUNT = 'Displays Count'


    # Template columns
    KPI_FAMILY = 'KPI Family'
    TARGET = 'Target'
    KPI_LVL_2_NAME = 'KPI Level 2 Name'
    KPI_LVL_3_NAME = 'KPI Level 3 Name'
    TEMPLATE_NAME_T = 'Template Name'
    TIERED = 'Tiered'
    RELATIVE_SCORE = 'Relative Score'
    BINARY = 'Binary'
    SCORE_LOGIC = 'score_logic'
    WEIGHT = 'Weight'
    MARS = 'MARS GCC'
    KPI_COMBINATION = 'Kpi Combination'
    PARENT_KPI = 'kpi_parent'
    CHILD_KPI = 'kpi_child'
    KPI_TYPE = 'kpi_type'
    PARAM_1_NUM_TYPE = 'param_type_1/numerator_type'
    PARAM_1_NUM_VALUE = 'param_value_1/numerator_value'
    PARAM_2_DENOM_TYPE = 'param_type_2/denom_type'
    PARAM_2_DENOM_VALUE = 'param_value_2/denom_value'
    EXCLUDE_TYPE_1 = 'exclude_param_type_1'
    EXCLUDE_VALUE_1 = 'exclude_param_value_1'
    EXCLUDE_TYPE_2 = 'exclude_param_type_2'
    EXCLUDE_VALUE_2 = 'exclude_param_value_2'
    EXCLUDE_EXCEPTION_TYPE_2 = 'exclude_param_2_exception_type'
    EXCLUDE_EXCEPTION_VALUE_2 = 'exclude_param_2_exception_value'

    TOTAL_UAE_SCORE = 'Total UAE Score'
    FIXED_TARGET_FOR_MR = 80
    OOS = 'OOS'
    DISTRIBUTED = 'DISTRIBUTED'

    # scif / matches columns
    SCENE_FK = 'scene_fk'
    PRODUCT_FK = 'product_fk'

    POPULATION = 'population'
    EXCLUDE = 'exclude'
    DENOM_FILTERS = 'denom_filters'
    NUM_FILTERS = 'num_filters'
    INCLUDE = 'include'
    DUPLICATE_SUFFIX = '_2'
    DUPLIC_KPI_FK = 'duplicate_kpi_fk'
    DUPLIC_KPI_TYPE = 'duplic_kpi_type'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.probe_groups = self.get_probe_group()
        self.match_product_in_scene = self.match_product_in_scene.merge(self.probe_groups, on='probe_match_fk',
                                                                        how='left')
        self.matches_products = self.match_product_in_scene.merge(self.all_products, on=self.PRODUCT_FK, how='left')
        self.external_targets = self.get_all_kpi_external_targets()
        self.all_targets_unpacked = self.unpack_all_external_targets()
        self.full_store_info = self.get_store_data_by_store_id()
        self.store_info_dict = self.full_store_info.to_dict('records')[0]
        self.category_params = self.get_category_level_targets()
        self.kpi_category_df = self.get_kpi_category_df()
        self.atomic_function = self.map_atomic_type_to_function()
        self.score_function = self.map_score_logic_to_function()
        self.assortment = Assortment(self.data_provider, self.output)
        self.block = Block(self.data_provider, self.output)
        self.lvl3_assortment = self.get_lvl3_relevant_assortment()
        self.own_manuf_fk = self.get_own_manufacturer_fk()
        self.atomic_kpi_results = pd.DataFrame(columns=['kpi_fk', self.KPI_TYPE, 'result', 'score', 'weight',
                                               'score_by_weight', 'parent_name'])
        self.atomic_tiers_df = pd.DataFrame()
        self.cat_lvl_res = pd.DataFrame()
        self.kpi_result_values = self.get_kpi_result_values_df()
        self.total_score = 0
        self.scenes_in_session = self.get_all_scenes_in_session()

    def get_all_scenes_in_session(self):
        scenes_in_ses = self.data_provider[Data.SCENES_INFO][[ScifConsts.SCENE_FK, ScifConsts.TEMPLATE_FK]]
        scenes_in_ses = scenes_in_ses.merge(self.data_provider[Data.ALL_TEMPLATES],
                                                on=ScifConsts.TEMPLATE_FK, how='left')
        return scenes_in_ses

    def get_kpi_result_values_df(self):
        query = MARSUAE_Queries.get_kpi_result_values()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_oos_distributed_result(self, result):
        value = self.OOS if not result else self.DISTRIBUTED
        custom_score = self.get_kpi_result_value_pk_by_value(value)
        return custom_score

    def get_own_manufacturer_fk(self):
        own_manufacturer_fk = self.data_provider.own_manufacturer.param_value.values[0]
        # own_manufacturer_fk = self.all_products[self.all_products['manufacturer_name'] ==
        #                                         'MARS GCC']['manufacturer_fk'].values[0]
        return own_manufacturer_fk

    def get_lvl3_relevant_assortment(self):
        assortment_result = self.assortment.get_lvl3_relevant_ass()
        if not assortment_result.empty:
            kpi_static_data = self.kpi_static_data[['pk', 'type']]
            assortment_result = assortment_result.merge(kpi_static_data, left_on='kpi_fk_lvl2', right_on='pk', how='left')
            assortment_result = assortment_result.drop(columns=['pk'])
            assortment_result.rename(columns={'type': 'ass_lvl2_kpi_type'}, inplace=True)
        return assortment_result

    def get_category_level_targets(self):
        category_params = self.all_targets_unpacked[self.all_targets_unpacked['operation_type'] == self.CATEGORY_LEVEL]
        return category_params

    def get_probe_group(self):
        query = MARSUAE_Queries.get_probe_group(self.session_uid)
        probe_group = pd.read_sql_query(query, self.rds_conn.db)
        return probe_group

    def get_kpi_category_df(self):
        kpi_category = []
        for i, row in self.category_params.iterrows():
            if isinstance(row['Template Group'], (list, tuple)):
                map(lambda x: kpi_category.append({'template_group': x, self.KPI_LVL_2_NAME: row['type']}),
                    row['Template Group'])
            else:
                kpi_category.append({'template_group': row['Template Group'], self.KPI_LVL_2_NAME: row['type']})
        kpi_category_df = pd.DataFrame.from_records(kpi_category)
        return kpi_category_df

    def get_store_data_by_store_id(self):
        store_id = self.store_id if self.store_id else self.session_info['store_fk'].values[0]
        query = MARSUAE_Queries.get_store_data_by_store_id(store_id)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_all_kpi_external_targets(self):
        query = MARSUAE_Queries.get_kpi_external_targets(self.visit_date)
        external_targets = pd.read_sql_query(query, self.rds_conn.db)
        return external_targets

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
            output_targets[self.WEIGHT] = output_targets[self.WEIGHT].apply(lambda x: float(x))
        if output_targets.empty:
            Log.warning('KPI External Targets Results are empty')
        return output_targets

    def get_store_atomic_kpi_parameters(self):
        atomic_params = self.external_targets[self.external_targets['operation_type'] == self.ATOMIC_LEVEL]
        atomic_params = atomic_params.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk', self.KPI_TYPE,
                                                              'key_json', 'data_json'], keep='last')
        relevant_atomic_df = pd.DataFrame(columns=atomic_params.columns.values.tolist())
        if not atomic_params.empty:
            policies_df = self.unpack_external_targets_json_fields_to_df(atomic_params, field_name='key_json')
            policy_columns = policies_df.columns.values.tolist()
            del policy_columns[policy_columns.index('pk')]
            match_dict = self.match_policy_attr_columns_to_value_columns(policy_columns)
            policies_df['policy_json'] = policies_df.apply(self.build_policy_json, args=(match_dict,), axis=1)
            atomic_params_pks = self.get_the_list_of_store_relevant_external_targets(policies_df)
            relevant_atomic_df = atomic_params[atomic_params['pk'].isin(atomic_params_pks)]
            data_json_df = self.unpack_external_targets_json_fields_to_df(relevant_atomic_df, 'data_json')
            if not data_json_df.empty:
                relevant_atomic_df = relevant_atomic_df.merge(data_json_df, on='pk', how='left')
                relevant_atomic_df[self.WEIGHT] = relevant_atomic_df[self.WEIGHT].apply(lambda x: float(x))
                relevant_atomic_df['kpi_parent'] = relevant_atomic_df['kpi_parent'].apply(lambda x: x if x else None)
                relevant_atomic_df['kpi_child'] = relevant_atomic_df['kpi_child'].apply(lambda x: x if x else None)
                relevant_atomic_df[self.TARGET] = relevant_atomic_df.apply(self.process_targets, axis=1)
        if relevant_atomic_df.empty:
            Log.warning('No atomic kpis are defined for store')
        return relevant_atomic_df

    def process_targets(self, row):
        target = row[self.TARGET]
        if row[self.SCORE_LOGIC] in [self.BINARY, self.RELATIVE_SCORE]:
            if target != target or target is None:
                self.log_target_error(row[self.KPI_TYPE])
                return 0
            try:
                target = float(target)
            except ValueError:
                self.log_target_error(row[self.KPI_TYPE])
                target = 0
        return target

    def get_the_list_of_store_relevant_external_targets(self, policies_df):
        atomic_params_pks = []
        for i, row in policies_df.iterrows():
            policy_fits = False
            for key, value in row['policy_json'].items():
                value = value if isinstance(value, (list, tuple)) else [value]
                if self.store_info_dict[key] in value:
                    policy_fits = True
                else:
                    policy_fits = False
            if policy_fits:
                atomic_params_pks.append(row['pk'])
        return atomic_params_pks

    @staticmethod
    def build_policy_json(row, match_dict):
        policy_dict = {}
        for key, value in match_dict.items():
            if row[key]:
                if row[key] == row[key] and row[key] is not None:
                    policy_dict.update({row[key]: row[value]})
        return policy_dict

    def build_tiers_for_atomics(self, atomics_df):
        filtered_atomics_df = atomics_df[atomics_df[self.SCORE_LOGIC] == self.TIERED]
        tier_dict_list = list()
        if not filtered_atomics_df.empty:
            filtered_atomics_df.apply(self.match_tier_targets_to_scores, args=(tier_dict_list,), axis=1)
        self.atomic_tiers_df = pd.DataFrame.from_records(tier_dict_list)

    def map_atomic_type_to_function(self):
        mapper = {self.AVAILABILITY: self.calculate_availability,
                  self.LINEAR_SOS: self.calculate_linear_sos,
                  self.DISPLAYS_COUNT: self.calculate_displays,
                  self.CHECKOUTS_COUNT: self.calculate_checkouts,
                  self.BLOCK: self.calculate_block,
                  self.KPI_COMBINATION: self.calculate_kpi_combination_score}
        return mapper

    def map_score_logic_to_function(self):
        mapper = {self.BINARY: self.get_binary_score, self.TIERED: self.get_tiered_score,
                  self.RELATIVE_SCORE: self.get_relative_score}
        return mapper

    @staticmethod
    def split_and_strip(value):
        return map(lambda x: x.strip(), str(value).split(','))

    @staticmethod
    def unpack_external_targets_json_fields_to_df(input_df, field_name):
        data_list = []
        for i, row in input_df.iterrows():
            data_item = json.loads(row[field_name]) if row[field_name] else {}
            data_item.update({'pk': row.pk})
            data_list.append(data_item)
        output_df = pd.DataFrame(data_list)
        return output_df

    def add_kpi_result_to_kpi_results_df(self, result_list):
        self.atomic_kpi_results.loc[len(self.atomic_kpi_results)] = result_list

    def get_kpi_type_by_pk(self, kpi_fk):
        try:
            kpi_fk = int(float(kpi_fk))
            return self.kpi_static_data[self.kpi_static_data['pk'] == kpi_fk]['type'].values[0]
        except IndexError:
            Log.info("Kpi fk: {} is not equal to any kpi type in static table".format(kpi_fk))
            return None

    def get_category_parent_dict(self, row):
        return {'kpi_fk': self.common.get_kpi_fk_by_kpi_type(row[self.KPI_LVL_2_NAME])}

    def match_tier_targets_to_scores(self, row, tier_dict_list):
        relevant_columns = filter(lambda x: x.startswith('score_cond'), row.index.values)
        for column in relevant_columns:
            if row[column] or row[column] == 0:
                if column.startswith('score_cond_target_'):
                    condition_number = str(column.strip('score_cond_target_'))
                    matching_value_col = filter(lambda x: x == 'score_cond_score_{}'.format(condition_number),
                                                relevant_columns)
                    value_col = matching_value_col[0] if len(matching_value_col) > 0 else None
                    if value_col or value_col == 0:
                        tier_dict = {self.KPI_TYPE: row[self.KPI_TYPE], 'step_value': row[column],
                                     'step_score_value': row[value_col]}
                        tier_dict_list.append(tier_dict)

    @staticmethod
    def match_policy_attr_columns_to_value_columns(policy_columns):
        attr_name_match_dict = {}
        for col in policy_columns:
            if col.startswith('store_att_name_'):
                condition_number = str(col.strip('store_att_name_'))
                matching_value_col = filter(lambda x: x == 'store_att_value_{}'.format(condition_number),
                                            policy_columns)
                value_col = matching_value_col[0] if len(matching_value_col) > 0 else None
                if value_col:
                    attr_name_match_dict.update({col: value_col})
        return attr_name_match_dict

    def get_general_filters(self, param_row, scif_flag=True):
        templates = param_row[self.TEMPLATE_NAME_T]
        if templates:
            conditions = {'location': {'template_name': templates}}
            relevant_scenes = filter_df(conditions, self.scif)[self.SCENE_FK].unique().tolist() if scif_flag else \
                filter_df(conditions, self.scenes_in_session)[self.SCENE_FK].unique().tolist()
        else:
            relevant_scenes = self.scif[self.SCENE_FK].unique().tolist() if scif_flag else \
                self.scenes_in_session[self.SCENE_FK].unique().tolist()
        general_filters = {'location': {self.SCENE_FK: relevant_scenes}}
        return general_filters

    def get_non_sos_kpi_filters(self, param_row):
        include_filters = {}
        if param_row[self.PARAM_1_NUM_TYPE]:
            include_filters.update({param_row[self.PARAM_1_NUM_TYPE]: param_row[self.PARAM_1_NUM_VALUE]})
        if param_row[self.PARAM_2_DENOM_TYPE]:
            include_filters.update({param_row[self.PARAM_2_DENOM_TYPE]: param_row[self.PARAM_2_DENOM_VALUE]})
        condition_filters = {self.POPULATION: {self.INCLUDE: [include_filters]}}
        exclude_filters = {}
        if param_row[self.EXCLUDE_TYPE_1]:
            exclude_filters.update({param_row[self.EXCLUDE_TYPE_1]: param_row[self.EXCLUDE_VALUE_1]})
        if param_row.get(self.EXCLUDE_TYPE_2) and param_row[self.EXCLUDE_TYPE_2] == param_row[self.EXCLUDE_TYPE_2]:
            exclude_filters.update({param_row[self.EXCLUDE_TYPE_2]: param_row[self.EXCLUDE_VALUE_2]})
        if exclude_filters:
            condition_filters[self.POPULATION].update({self.EXCLUDE: exclude_filters})
        return condition_filters

    def get_sos_filters(self, param_row):
        if not param_row[self.PARAM_2_DENOM_TYPE] or not param_row[self.PARAM_1_NUM_TYPE]:
            Log.error('Sos filters are incorrect for kpi {}. '
                      'Kpi is not calculated'.format(param_row[self.KPI_TYPE]))
            return None, None
        denominator_filters = {}
        if param_row[self.PARAM_2_DENOM_TYPE]:
            denominator_filters.update({param_row[self.PARAM_2_DENOM_TYPE]: param_row[self.PARAM_2_DENOM_VALUE]})
        sos_filters = {self.POPULATION: {self.INCLUDE: [denominator_filters]}}

        exclude_filters = {}
        if param_row[self.EXCLUDE_TYPE_1]:
            exclude_filters.update({param_row[self.EXCLUDE_TYPE_1]: param_row[self.EXCLUDE_VALUE_1]})
        if exclude_filters:
            sos_filters[self.POPULATION].update({self.EXCLUDE: exclude_filters})

        num_filters = {}
        if param_row[self.PARAM_1_NUM_TYPE]:
            num_filters.update({param_row[self.PARAM_1_NUM_TYPE]: param_row[self.PARAM_1_NUM_VALUE]})
        numerator_filters = copy.deepcopy(sos_filters)
        numerator_filters[self.POPULATION][self.INCLUDE][0].update(num_filters)

        final_filters = {self.DENOM_FILTERS: sos_filters, self.NUM_FILTERS: numerator_filters}

        if param_row.get(self.EXCLUDE_TYPE_2) and (param_row[self.EXCLUDE_TYPE_2] == param_row[self.EXCLUDE_TYPE_2]):
            if not exclude_filters:
                final_filters.get(self.DENOM_FILTERS).get(self.POPULATION)[self.EXCLUDE] = exclude_filters
                final_filters.get(self.NUM_FILTERS).get(self.POPULATION)[self.EXCLUDE] = exclude_filters
            main_filters, additional_filters = self.update_sos_filters_with_additional_filters(final_filters, param_row)
            return main_filters, additional_filters
        return final_filters, None
        # filters = {'location': {'scene_fk': [27898, 27896, 27894, 27859, 27892, 27854]},
        #            'population': {'exclude': {'template_name': 'Checkout Chocolate'},
        #                           'include': [{'category_fk': 6, 'manufacturer_fk': 3}]}}

    def update_sos_filters_with_additional_filters(self, final_filters, param_row):
        main_filters = copy.deepcopy(final_filters)
        main_denom_exclude_filters = main_filters.get(self.DENOM_FILTERS).get(self.POPULATION).get(self.EXCLUDE)
        if param_row[self.EXCLUDE_TYPE_2] != param_row[self.EXCLUDE_TYPE_1]:
            main_denom_exclude_filters.update({param_row[self.EXCLUDE_TYPE_2]: param_row[self.EXCLUDE_VALUE_2]})
        else:
            excl_value_list = [param_row[self.EXCLUDE_VALUE_1]]
            excl_value_list.append(param_row[self.EXCLUDE_VALUE_2])
            main_denom_exclude_filters.update({param_row[self.EXCLUDE_TYPE_1]: excl_value_list})

        main_num_exclude_filters = main_filters.get(self.NUM_FILTERS).get(self.POPULATION).get(self.EXCLUDE)
        if param_row[self.EXCLUDE_TYPE_2] != param_row[self.EXCLUDE_TYPE_1]:
            main_num_exclude_filters.update({param_row[self.EXCLUDE_TYPE_2]: param_row[self.EXCLUDE_VALUE_2]})
        else:
            excl_value_list = [param_row[self.EXCLUDE_VALUE_1]]
            excl_value_list.append(param_row[self.EXCLUDE_VALUE_2])
            main_num_exclude_filters.update({param_row[self.EXCLUDE_TYPE_1]: excl_value_list})

        if not param_row[self.EXCLUDE_EXCEPTION_TYPE_2] or not\
                param_row[self.EXCLUDE_EXCEPTION_TYPE_2] == param_row[self.EXCLUDE_EXCEPTION_TYPE_2]:
            return main_filters, None

        additional_filters = copy.deepcopy(final_filters)
        additional_filters.get(self.DENOM_FILTERS).get(self.POPULATION).get(self.INCLUDE)[0]. \
            update({param_row[self.EXCLUDE_TYPE_2]: param_row[self.EXCLUDE_VALUE_2],
                    param_row[self.EXCLUDE_EXCEPTION_TYPE_2]: param_row[self.EXCLUDE_EXCEPTION_VALUE_2]})
        additional_filters.get(self.NUM_FILTERS).get(self.POPULATION).get(self.INCLUDE)[0]. \
            update({param_row[self.EXCLUDE_TYPE_2]: param_row[self.EXCLUDE_VALUE_2],
                    param_row[self.EXCLUDE_EXCEPTION_TYPE_2]: param_row[self.EXCLUDE_EXCEPTION_VALUE_2]})
        return main_filters, additional_filters

    def get_kpi_result_value_pk_by_value(self, value):
        pk = None
        try:
            pk = self.kpi_result_values[self.kpi_result_values['value'] == value]['pk'].values[0]
        except IndexError as e:
            Log.error('Value {} does not exist'.format(value))
        return pk

    def add_duplicate_kpi_fk_where_applicable(self, store_atomics):
        store_atomics[self.DUPLIC_KPI_TYPE] = store_atomics[self.KPI_TYPE].apply(lambda x: '{}{}'.format(x,
                                                                                                self.DUPLICATE_SUFFIX))
        kpi_static_data = self.kpi_static_data[['pk', 'type']]
        kpi_static_data.rename(columns={'pk': self.DUPLIC_KPI_FK, 'type': self.DUPLIC_KPI_TYPE}, inplace=True)
        store_atomics = store_atomics.merge(kpi_static_data, on=self.DUPLIC_KPI_TYPE, how='left')
        store_atomics[self.DUPLIC_KPI_FK] = store_atomics[self.DUPLIC_KPI_FK].apply(lambda x: x if x == x else '')
        return store_atomics
        
#-------------------------------main calculation section-----------------------------------

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.calculate_atomics()
        self.calculate_category_level()
        self.calculate_total_score()

    def calculate_total_score(self):
        if not self.cat_lvl_res.empty:
            sum_weights = float(self.cat_lvl_res[self.WEIGHT].sum())
            self.cat_lvl_res['weighted_scores'] = self.cat_lvl_res['cat_score'] * self.cat_lvl_res[self.WEIGHT]
            total_result = self.cat_lvl_res['weighted_scores'].sum() / sum_weights
            self.total_score = total_result

        kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.TOTAL_UAE_SCORE)
        identifier_result = {'kpi_fk': kpi_fk}
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                       result=self.total_score, score=self.total_score,
                                       identifier_result=identifier_result, target=self.FIXED_TARGET_FOR_MR,
                                       should_enter=True)

    def calculate_category_level(self):
        self.cat_lvl_res = self.atomic_kpi_results.groupby(['parent_name'],
                                                           as_index=False).agg({'score_by_weight': np.sum})
        self.cat_lvl_res.rename(columns={'parent_name': self.KPI_TYPE, 'score_by_weight': 'cat_score'}, inplace=True)
        self.cat_lvl_res = self.cat_lvl_res.merge(self.category_params, on='kpi_type', how='left')
        identifier_parent = {'kpi_fk': self.common.get_kpi_fk_by_kpi_type(self.TOTAL_UAE_SCORE)}
        for i, result in self.cat_lvl_res.iterrows():
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(result[self.KPI_TYPE])
            identifier_result = {'kpi_fk': kpi_fk}
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                           result=result['cat_score'], score=result['cat_score'], weight=100,
                                           identifier_parent=identifier_parent, identifier_result=identifier_result,
                                           target=self.FIXED_TARGET_FOR_MR, should_enter=True,
                                           numerator_result=result[self.WEIGHT])

    def get_atomics_for_template_groups_present_in_store(self, store_atomics):
        session_template_groups = self.scenes_in_session['template_group'].unique().tolist()
        category_kpis_df = self.kpi_category_df[self.kpi_category_df['template_group'].isin(session_template_groups)]
        visit_category_kpis = category_kpis_df[self.KPI_LVL_2_NAME].unique().tolist()
        store_atomics = store_atomics[store_atomics[self.KPI_LVL_2_NAME].isin(visit_category_kpis)]
        if store_atomics.empty:
            Log.warning('Template groups in session do not correspond to template groups for category level kpis')
        return store_atomics

    def calculate_atomics(self):
        store_atomics = self.get_store_atomic_kpi_parameters()
        if store_atomics.empty:
            Log.warning('No targets were set for store type of store {}'.format(self.store_id))
            return
        store_atomics = self.get_atomics_for_template_groups_present_in_store(store_atomics)
        if not store_atomics.empty:
            self.build_tiers_for_atomics(store_atomics)
            execute_list = Node.get_kpi_execute_list(store_atomics)
            store_atomics = store_atomics.reset_index(drop=True)
            store_atomics = self.add_duplicate_kpi_fk_where_applicable(store_atomics)
            for kpi in execute_list:
                i = store_atomics[store_atomics[self.KPI_TYPE] == kpi].index[0]
                row = store_atomics.iloc[i]
                self.calculate_atomic_results(row)

    def calculate_atomic_results(self, param_row):
        if param_row[self.KPI_TYPE] not in self.atomic_kpi_results[self.KPI_TYPE].values.tolist():
            kpi_type = param_row[self.KPI_FAMILY]
            self.atomic_function[kpi_type](param_row)

    @kpi_runtime()
    def calculate_availability(self, param_row):
        if self.lvl3_assortment.empty:
            Log.warning("Assortment list is empty for store type {}".format(self.store_info_dict['store_type']))
            self.write_zero_assortment_result(param_row)
            return
        lvl3_ass_res = self.lvl3_assortment[self.lvl3_assortment['ass_lvl2_kpi_type'] == param_row[self.KPI_TYPE]]
        if lvl3_ass_res.empty:
            Log.warning("Assortment list not available for kpi {}".format(param_row[self.KPI_TYPE]))
            self.write_zero_assortment_result(param_row)
        if not lvl3_ass_res.empty:
            lvl3_ass_res = self.calculate_lvl_3_assortment_result(lvl3_ass_res, param_row)
            self.calculate_lvl_2_assortment_result(lvl3_ass_res, param_row)

    def write_zero_assortment_result(self, param_row):
        identifier_cat_parent = self.get_category_parent_dict(param_row)
        kpi_fk = param_row['kpi_level_2_fk']
        result = 0
        score, weight = self.get_score(result, param_row)
        target = param_row[self.TARGET] * 100 if param_row[self.TARGET] else None
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.own_manuf_fk,
                                       numerator_result=0, result=result * 100,
                                       denominator_id=self.store_id, denominator_result=0,
                                       score=score * weight, weight=weight, target=target,
                                       identifier_parent=identifier_cat_parent, should_enter=True)
        self.add_kpi_result_to_kpi_results_df([kpi_fk, param_row[self.KPI_TYPE],
                                               result, score, weight, score * weight,
                                               param_row[self.KPI_LVL_2_NAME]])

    def calculate_lvl_3_assortment_result(self, lvl3_ass_res, param_row):
        lvl3_ass_res = self.get_template_relevant_assortment_result(lvl3_ass_res, param_row)
        for i, row in lvl3_ass_res.iterrows():
            identifier_parent = {'kpi_fk': row.kpi_fk_lvl2}
            custom_result = self.get_oos_distributed_result(row.in_store)
            self.common.write_to_db_result(fk=row.kpi_fk_lvl3, numerator_id=row.product_fk,
                                           numerator_result=row.facings, denominator_result=1,
                                           denominator_id=row.product_fk, result=custom_result,
                                           score=row.in_store, should_enter=True,
                                           identifier_parent=identifier_parent)
        return lvl3_ass_res

    def calculate_lvl_2_assortment_result(self, lvl3_ass_res, param_row):
        identifier_cat_parent = self.get_category_parent_dict(param_row)
        lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_ass_res)
        for i, row in lvl2_result.iterrows():
            identifier_result = {'kpi_fk': row.kpi_fk_lvl2}
            denominator_res = row.total
            if row.target and not np.math.isnan(row.target):
                if row.group_target_date <= self.visit_date:
                    denominator_res = row.target
            result = np.divide(float(row.passes), float(denominator_res))
            score, weight = self.get_score(result, param_row)
            target = param_row[self.TARGET] * 100 if param_row[self.TARGET] else None
            self.common.write_to_db_result(fk=row.kpi_fk_lvl2, numerator_id=self.own_manuf_fk,
                                           numerator_result=row.passes, result=result * 100,
                                           denominator_id=self.store_id, denominator_result=denominator_res,
                                           score=score * weight, weight=weight, target=target,
                                           identifier_result=identifier_result,
                                           identifier_parent=identifier_cat_parent, should_enter=True)
            self.add_kpi_result_to_kpi_results_df([row.kpi_fk_lvl2, param_row[self.KPI_TYPE],
                                                   result, score, weight, score * weight,
                                                   param_row[self.KPI_LVL_2_NAME]])

    def add_actual_facings_to_assortment(self,lvl3_ass_res, scif):
        lvl3_ass_res['facings'] = 0
        product_assort = lvl3_ass_res[self.PRODUCT_FK].unique()
        for prod in product_assort:
            lvl3_ass_res.loc[lvl3_ass_res[self.PRODUCT_FK] == prod, 'facings'] = scif[scif[self.PRODUCT_FK] \
                                                                                   == prod]['facings'].sum()

    def get_template_relevant_assortment_result(self, lvl3_ass_res, param_row):
        ass_template = param_row[self.TEMPLATE_NAME_T]
        filtered_scif = self.scif.copy()
        if ass_template:
            filtered_scif = self.scif[self.scif['template_name'].isin(ass_template)] \
                if isinstance(ass_template, (list, tuple)) else self.scif[self.scif['template_name'] == ass_template]
        products_in_session = filtered_scif.loc[filtered_scif['facings'] > 0][self.PRODUCT_FK].values
        lvl3_ass_res.loc[lvl3_ass_res[self.PRODUCT_FK].isin(products_in_session), 'in_store'] = 1
        self.add_actual_facings_to_assortment(lvl3_ass_res, filtered_scif)
        return lvl3_ass_res

    def get_score(self, result, param_row):
        kpi_logic = param_row[self.SCORE_LOGIC]
        score_function = self.score_function.get(kpi_logic)
        if score_function is not None:
            score = score_function(param_row, result)
        else:
            Log.error('Score logic {} for kpi {} is not supported. '
                      'Score set to zero'.format(kpi_logic, param_row[self.KPI_TYPE]))
            score = 0
        weight = float(param_row[self.WEIGHT])
        return score, weight

    def get_tiered_score(self, param_row, result):
        # in case the rule is >= step...
        kpi_name = param_row[self.KPI_TYPE]
        tiers = self.atomic_tiers_df[self.atomic_tiers_df[self.KPI_TYPE] == kpi_name]
        relevant_step = min(tiers[tiers['step_value'] > result]['step_value'].values.tolist())
        tier_score_value = tiers[tiers['step_value'] == relevant_step]['step_score_value'].values[0]
        score = tier_score_value
        return score

    def get_relative_score(self, param_row, result):
        target = float(param_row[self.TARGET])
        score = result / target if target else 0
        return score

    def get_binary_score(self, param_row, result):
        target = float(param_row[self.TARGET])
        score = 1 if result >= target else 0
        return score

    @kpi_runtime()
    def calculate_linear_sos(self, param_row):
        general_filters = self.get_general_filters(param_row)
        sos_filters, additional_filters = self.get_sos_filters(param_row)
        if sos_filters is not None:
            sos_filters[self.NUM_FILTERS].update(general_filters)
            sos_filters[self.DENOM_FILTERS].update(general_filters)
            numerator_length = self.calculate_linear_space(sos_filters[self.NUM_FILTERS])
            denominator_length = self.calculate_linear_space(sos_filters[self.DENOM_FILTERS])
            if additional_filters is not None:
                additional_filters[self.NUM_FILTERS].update(general_filters)
                additional_filters[self.DENOM_FILTERS].update(general_filters)
                add_numerator_length = self.calculate_linear_space(additional_filters[self.NUM_FILTERS])
                add_denominator_length = self.calculate_linear_space(additional_filters[self.DENOM_FILTERS])
                numerator_length += add_numerator_length
                denominator_length += add_denominator_length

            result = numerator_length / denominator_length if denominator_length else 0
            score, weight = self.get_score(result=result, param_row=param_row)

            num_id = param_row[self.PARAM_1_NUM_VALUE] if \
                        isinstance(param_row[self.PARAM_1_NUM_VALUE], (str, unicode, int, float)) else self.own_manuf_fk
            denom_id = param_row[self.PARAM_2_DENOM_VALUE]
            identifier_parent = self.get_identifier_parent_for_atomic(param_row)
            identifier_result = self.get_identifier_result_for_atomic(param_row)
            target = param_row[self.TARGET] * 100 if param_row[self.TARGET] else None
            self.common.write_to_db_result(fk=param_row['kpi_level_2_fk'], numerator_id=num_id,
                                           numerator_result=numerator_length, denominator_id=denom_id,
                                           denominator_result=denominator_length, target=target,
                                           result=result * 100, score=score * weight, weight=weight,
                                           identifier_parent=identifier_parent, identifier_result=identifier_result,
                                           should_enter=True)
            self.add_kpi_result_to_kpi_results_df([param_row['kpi_level_2_fk'], param_row[self.KPI_TYPE],
                                                   result * 100, score, weight, score * weight,
                                                   param_row[self.KPI_LVL_2_NAME]])
            if param_row[self.DUPLIC_KPI_FK]:
                self.common.write_to_db_result(fk=param_row[self.DUPLIC_KPI_FK], numerator_id=num_id,
                                               numerator_result=numerator_length, denominator_id=denom_id,
                                               denominator_result=denominator_length, target=target,
                                               result=result * 100, score=score * weight, weight=weight,
                                               identifier_parent=identifier_result, should_enter=True)

    @kpi_runtime()
    def calculate_displays(self, param_row):
        general_filters = self.get_general_filters(param_row, scif_flag=False)
        filtered_scenes = filter_df(general_filters, self.scenes_in_session)
        result = len(filtered_scenes[self.SCENE_FK].unique())
        score, weight = self.get_score(result, param_row)
        identifier_parent = self.get_identifier_parent_for_atomic(param_row)
        target = param_row[self.TARGET] if param_row[self.TARGET] else None
        identifier_result = self.get_identifier_result_for_atomic(param_row)
        self.common.write_to_db_result(fk=param_row['kpi_level_2_fk'], numerator_id=self.own_manuf_fk,
                                       numerator_result=result, result=result, target=target,
                                       denominator_id=self.store_id, score=score * weight, weight=weight,
                                       identifier_parent=identifier_parent, identifier_result=identifier_result,
                                       should_enter=True)
        self.add_kpi_result_to_kpi_results_df([param_row['kpi_level_2_fk'], param_row['kpi_type'],
                                               result, score, weight, score * weight,
                                               param_row[self.KPI_LVL_2_NAME]])

    def get_identifier_parent_for_atomic(self, param_row):
        parent_fk = self.common.get_kpi_fk_by_kpi_type(param_row[self.PARENT_KPI]) if param_row[self.PARENT_KPI] else \
                                                    self.common.get_kpi_fk_by_kpi_type(param_row[self.KPI_LVL_2_NAME])
        identifier_parent = {'kpi_fk': parent_fk}
        return identifier_parent

    def get_identifier_result_for_atomic(self, param_row):
        identifier_result = None
        if param_row[self.CHILD_KPI] or param_row[self.DUPLIC_KPI_FK]:
            identifier_result = {'kpi_fk': param_row['kpi_level_2_fk']}
        return identifier_result

    @kpi_runtime()
    def calculate_checkouts(self, param_row):
        denom_filters = self.get_general_filters(param_row, scif_flag=False)
        filtered_scenes = filter_df(denom_filters, self.scenes_in_session)
        all_ch_o = len(filtered_scenes[self.SCENE_FK].unique())

        filters = self.get_general_filters(param_row)
        kpi_filters = self.get_non_sos_kpi_filters(param_row)
        filters.update(kpi_filters)
        filtered_matches = filter_df(filters, self.matches_products)
        filtered_matches = filtered_matches[filtered_matches['stacking_layer'] == 1]

        scene_probe_groups = len(filtered_matches.drop_duplicates(subset=[self.SCENE_FK]))
        result = float(scene_probe_groups) / all_ch_o if all_ch_o else 0
        score, weight = self.get_score(param_row=param_row, result=result)
        target = param_row[self.TARGET] * 100 if param_row[self.TARGET] else None
        identifier_parent = self.get_identifier_parent_for_atomic(param_row)
        identifier_result = self.get_identifier_result_for_atomic(param_row)
        self.common.write_to_db_result(fk=param_row['kpi_level_2_fk'], numerator_id=self.own_manuf_fk,
                                       numerator_result=scene_probe_groups, denominator_result=all_ch_o,
                                       result=result * 100, target=target,
                                       denominator_id=self.store_id, score=score * weight, weight=weight,
                                       identifier_parent=identifier_parent, identifier_result=identifier_result,
                                       should_enter=True)
        self.add_kpi_result_to_kpi_results_df([param_row['kpi_level_2_fk'], param_row['kpi_type'],
                                               result * 100, score, weight, score * weight,
                                               param_row[self.KPI_LVL_2_NAME]])
        if param_row[self.DUPLIC_KPI_FK]:
            self.common.write_to_db_result(fk=param_row[self.DUPLIC_KPI_FK], numerator_id=self.own_manuf_fk,
                                           numerator_result=scene_probe_groups, denominator_result=all_ch_o,
                                           result=result * 100, target=target,
                                           denominator_id=self.store_id, score=score * weight, weight=weight,
                                           identifier_parent=identifier_result, should_enter=True)

    @kpi_runtime()
    def calculate_block(self, param_row):
        if self.lvl3_assortment.empty:
            return
        block_ass = self.lvl3_assortment[self.lvl3_assortment['ass_lvl2_kpi_type'] == param_row[self.KPI_TYPE]]
        if not block_ass.empty:
            block_ass = self.get_template_relevant_assortment_result(block_ass, param_row)
            if not block_ass.empty:
                relevant_products = block_ass[self.PRODUCT_FK].unique().tolist()
                skus_in_clusters = self.get_relevant_block_clusters(relevant_products, param_row)
                cluster_results = list()
                number_of_products = float(len(relevant_products))
                for result in skus_in_clusters:
                    cluster_results.append(result / number_of_products)
                result = max(cluster_results) if cluster_results else 0
                score, weight = self.get_score(result, param_row)

                target = param_row[self.TARGET] * 100 if param_row[self.TARGET] else None
                identifier_parent = self.get_identifier_parent_for_atomic(param_row)
                identifier_result = self.get_identifier_result_for_atomic(param_row)
                self.common.write_to_db_result(fk=param_row['kpi_level_2_fk'], numerator_id=self.own_manuf_fk,
                                               numerator_result=result, result=result * 100, target=target,
                                               denominator_id=self.store_id, score=score * weight, weight=weight,
                                               identifier_parent=identifier_parent, identifier_result=identifier_result,
                                               should_enter=True)
                self.add_kpi_result_to_kpi_results_df([param_row['kpi_level_2_fk'], param_row[self.KPI_TYPE],
                                                       result, score, weight, score * weight,
                                                       param_row[self.KPI_LVL_2_NAME]])

    def get_relevant_block_clusters(self, relevant_products, param_row):
        general_filters = self.get_general_filters(param_row)
        scenes_template = general_filters['location'][self.SCENE_FK]
        scenes = self.scif[(self.scif[self.SCENE_FK].isin(scenes_template)) &
                           (self.scif[self.PRODUCT_FK].isin(relevant_products))][self.SCENE_FK].unique().tolist()
        block_filters = {self.PRODUCT_FK: relevant_products}
        additional_filters = {'minimum_facing_for_block': 2, 'minimum_block_ratio': 0}
        cluster_skus_list = list()
        for scene in scenes:
            location_filters = {self.SCENE_FK: [scene]}
            block_res = self.block.network_x_block_together(block_filters, location_filters, additional_filters)
            block_res = block_res[block_res['is_block'] == True]
            if block_res.empty:
                continue
            else:
                self.get_number_of_sku_types_from_clusters(block_res, cluster_skus_list)
        return cluster_skus_list

    def get_number_of_sku_types_from_clusters(self, block_res, cluster_skus_list):
        for i, block_row in block_res.iterrows():
            cluster_nodes = block_row['cluster'].nodes.values() # returns the list of dictionaries for each cluster item
            sku_types = set()
            product_list = [list(node[self.PRODUCT_FK]) for node in cluster_nodes]
            map(lambda x: sku_types.update(x), product_list)
            cluster_skus_list.append(len(sku_types))

    @kpi_runtime()
    def calculate_kpi_combination_score(self, param_row):
        child_kpis = param_row[self.CHILD_KPI] if isinstance(param_row[self.CHILD_KPI], (list, tuple)) \
                                                    else [param_row[self.CHILD_KPI]]
        child_results = self.atomic_kpi_results[self.atomic_kpi_results[self.KPI_TYPE].isin(child_kpis)]
        result = len(child_results[(child_results['score'] == 1) | (child_results['score'] == 100)])
        score, weight = self.get_score(result, param_row)
        target = param_row[self.TARGET] if param_row[self.TARGET] else None
        identifier_parent = self.get_identifier_parent_for_atomic(param_row)
        identifier_result = self.get_identifier_result_for_atomic(param_row)
        self.common.write_to_db_result(fk=param_row['kpi_level_2_fk'], numerator_id=self.own_manuf_fk,
                                       numerator_result=result, result=score*100, target=target,
                                       denominator_id=self.store_id, score=score * weight, weight=weight,
                                       identifier_parent=identifier_parent, identifier_result=identifier_result,
                                       should_enter=True)
        self.add_kpi_result_to_kpi_results_df([param_row['kpi_level_2_fk'], param_row[self.KPI_TYPE],
                                               score * 100, score, weight, score * weight,
                                               param_row[self.KPI_LVL_2_NAME]])

    def calculate_linear_space(self, filters):
        filtered_scif = filter_df(filters, self.scif)
        space_length = filtered_scif['gross_len_ign_stack'].sum()
        return float(space_length)

    @staticmethod
    def log_target_error(kpi_type):
        Log.error('Target is not set accordingly for kpi {}'.format(kpi_type))
