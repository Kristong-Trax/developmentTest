
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import json
import numpy as np
import os

from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from KPIUtils_v2.Utils.Parsers.ParseInputKPI import filter_df
from Projects.MARSUAE_SAND.Utils.Fetcher import MARSUAE_SAND_Queries

__author__ = 'natalyak'

# KPI_RESULT = 'report.kpi_results'
# KPK_RESULT = 'report.kpk_results'
# KPS_RESULT = 'report.kps_results'


class MARSUAE_SANDToolBox:
    # LEVEL1 = 1
    # LEVEL2 = 2
    # LEVEL3 = 3
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

    TOTAL_UAE_SCORE = 'Total UAE Score'

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
        self.matches_products = self.match_product_in_scene.merge(self.all_products, on='product_fk', how='left')
        self.external_targets = self.get_all_kpi_external_targets()
        self.all_targets_unpacked = self.unpack_all_external_targets()
        self.full_store_info = self.get_store_data_by_store_id()
        self.store_info_dict = self.full_store_info.to_dict('records')[0]
        self.category_params = self.get_category_level_targets()
        # self.kpi_category_dict = self.get_kpi_category_dict()
        self.atomic_function = self.map_atomic_type_to_function()
        self.score_function = self.map_score_logic_to_function()
        self.assortment = Assortment(self.data_provider, self.output)
        self.block = Block(self.data_provider, self.output)
        self.lvl3_assortment = self.get_lvl3_relevant_assortment()
        self.own_manuf_fk = self.all_products[self.all_products['manufacturer_name'] ==
                                              self.MARS]['manufacturer_fk'].values[0]
        self.atomic_kpi_results = pd.DataFrame(columns=['kpi_fk', 'kpi_type', 'result', 'score', 'weight',
                                               'score_by_weight', 'parent_name'])
        self.atomic_tiers_df = pd.DataFrame()
        self.cat_lvl_res = pd.DataFrame()
        # self.kpi_results_queries = []

    def get_lvl3_relevant_assortment(self):
        assortment_result = self.assortment.get_lvl3_relevant_ass()
        kpi_static_data = self.kpi_static_data[['pk', 'type']]
        assortment_result = assortment_result.merge(kpi_static_data, left_on='kpi_fk_lvl2', right_on='pk', how='left')
        assortment_result = assortment_result.drop(columns=['pk'])
        assortment_result.rename(columns={'type': 'ass_lvl2_kpi_type'}, inplace=True)
        return assortment_result

    def get_category_level_targets(self):
        category_params = self.all_targets_unpacked[self.all_targets_unpacked['operation_type'] == self.CATEGORY_LEVEL]
        return category_params

    def get_probe_group(self):
        query = MARSUAE_SAND_Queries.get_probe_group(self.session_uid)
        probe_group = pd.read_sql_query(query, self.rds_conn.db)
        return probe_group

    # def get_kpi_category_dict(self):
    #     kpi_category_dict = {}
    #     for i, row in self.category_params.iterrows():
    #         if isinstance(row['Template Name'], (list, tuple)):
    #             map(lambda x: kpi_category_dict.update({x: row['kpi_level_2_fk']}), row['Template Name'])
    #         else:
    #             kpi_category_dict.update({row['Template Name']: row['kpi_level_2_fk']})
    #     return kpi_category_dict

    def get_tier_score_steps(self, external_targets_df):

        pass

    def get_store_data_by_store_id(self):
        store_id = self.store_id if self.store_id else self.session_info['store_fk'].values[0]
        query = MARSUAE_SAND_Queries.get_store_data_by_store_id(store_id)
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_all_kpi_external_targets(self):
        query = MARSUAE_SAND_Queries.get_kpi_external_targets(self.visit_date)
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
        atomic_params = atomic_params.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk', 'kpi_type',
                                                              'key_json', 'data_json'], keep='last')
        relevant_atomic_params_df = pd.DataFrame(columns=atomic_params.columns.values.tolist())
        if not atomic_params.empty:
            policies_df = self.unpack_external_targets_json_fields_to_df(atomic_params, field_name='key_json')
            policy_columns = policies_df.columns.values.tolist()
            del policy_columns[policy_columns.index('pk')]

            match_dict = self.match_policy_attr_columns_to_value_columns(policy_columns)
            policies_df['policy_json'] = policies_df.apply(self.build_policy_json, args=(match_dict,), axis=1)
            # for column in policy_columns:
            #     store_att_value = self.store_info_dict.get(column)
            #     mask = policies_df.apply(self.get_masking_filter, args=(column, store_att_value), axis=1)
            #     policies_df = policies_df[mask]
            #
            # atomic_params_pks = policies_df['pk'].values.tolist()
            atomic_params_pks = self.get_the_list_of_store_relevant_external_targets(policies_df)
            # atomic_params_pks = []
            # for i, row in policies_df.iterrows():
            #     policy_fits = False
            #     for key, value in row['policy_json'].items():
            #         value = value if isinstance(value, (list, tuple)) else [value]
            #         if self.store_info_dict[key] in value:
            #             policy_fits = True
            #         else:
            #             policy_fits = False
            #     if policy_fits:
            #         atomic_params_pks.append(row['pk'])
            relevant_atomic_params_df = atomic_params[atomic_params['pk'].isin(atomic_params_pks)]
            data_json_df = self.unpack_external_targets_json_fields_to_df(relevant_atomic_params_df, 'data_json')
            relevant_atomic_params_df = relevant_atomic_params_df.merge(data_json_df, on='pk', how='left')
            relevant_atomic_params_df[self.WEIGHT] = relevant_atomic_params_df[self.WEIGHT].apply(lambda x: float(x))
            relevant_atomic_params_df[self.CHILD_KPI] = relevant_atomic_params_df[self.CHILD_KPI].astype(object)
        return relevant_atomic_params_df

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

    def get_masking_filter(self, row, column, store_att_value):
        if store_att_value in self.split_and_strip(row[column]):
            return True
        else:
            return False

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
        return {'kpi_fk': row[self.KPI_LVL_2_NAME]}

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

    # def match_store_attributes_to_values(self, row, tier_dict_list):
    #     relevant_columns = filter(lambda x: x.startswith('score_cond'), row.index.values)
    #     for column in relevant_columns:
    #         if row[column]:
    #             if column.startswith('score_cond_target_'):
    #                 condition_number = str(column.strip('score_cond_target_'))
    #                 matching_value_col = filter(lambda x: x == 'score_cond_score_'.format(condition_number),
    #                                             relevant_columns)
    #                 value_col = matching_value_col[0] if len(matching_value_col) > 0 else None
    #                 if value_col:
    #                     tier_dict = {self.KPI_TYPE: row[self.KPI_TYPE], 'step_value': row[column],
    #                                  'step_score_value': row(value_col)}
    #                     tier_dict_list.append(tier_dict)

    def get_general_filters(self, param_row):
        templates = param_row[self.TEMPLATE_NAME_T]
        if templates:
            conditions = {'location': {'template_name': templates}}
            relevant_scenes = filter_df(conditions, self.scif)['scene_fk'].values.tolist()
        else:
            relevant_scenes = self.scif['scene_fk'].values.tolist()
        general_filters = {'location': {'scene_fk': relevant_scenes}}
        return general_filters

    @staticmethod
    def get_non_sos_kpi_filters(param_row):
        include_filters = {}
        if param_row['param_type_1/numerator_type']:
            include_filters.update({param_row['param_type_1/numerator_type']: param_row['param_value_1/numerator_value']})
        if param_row['param_type_2/denom_type']:
            include_filters.update({param_row['param_type_2/denom_type']: param_row['param_value_2/denom_value']})
        condition_filters = {'population': {'include': [include_filters]}}
        exclude_filters = {}
        if param_row['exclude_param_type_1']:
            exclude_filters.update({param_row['exclude_param_type_1']: param_row['exclude_param_value_1']})
        condition_filters['population'].update({'exclude': [exclude_filters]})
        return condition_filters

    def get_sos_filters(self, param_row):
        if not param_row['param_type_2/denom_type'] or not param_row['param_type_1/numerator_type']:
            Log.error('Sos filters are incorrect for kpi {}. '
                      'Kpi is not calculated'.format(param_row[self.KPI_TYPE]))
            return None
        denominator_filters = {}
        if param_row['param_type_2/denom_type']:
            denominator_filters.update({param_row['param_type_2/denom_type']: param_row['param_value_2/denom_value']})
        sos_filters = {'population': {'include': [denominator_filters]}}

        exclude_filters = {}
        if param_row['exclude_param_type_1']:
            exclude_filters.update({param_row['exclude_param_type_1']: param_row['exclude_param_value_1']})
        if exclude_filters:
            sos_filters['population'].update({'exclude': [exclude_filters]})

        num_filters = {}
        if param_row['param_type_1/numerator_type']:
            num_filters.update({param_row['param_type_1/numerator_type']: param_row['param_value_1/numerator_value']})
        numerator_filters = sos_filters.copy()
        numerator_filters['population']['include'].append(num_filters)

        final_filters = {'denom_filters': sos_filters, 'num_filters': numerator_filters}
        return final_filters
        
#-------------------------------main calculation section-----------------------------------

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.calculate_atomics()
        self.calculate_category_level()
        self.calculate_total_score()

    def calculate_total_score(self):
        category_results = self.cat_lvl_res.merge(self.get_category_level_targets, on='kpi_type', how='left')
        sum_weights = float(category_results[self.WEIGHT].sum())
        category_results['weighted_scores'] = category_results['cat_score'] * category_results[self.WEIGHT]
        total_result = category_results['weighted_scores'].sum() / sum_weights
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.TOTAL_UAE_SCORE)
        identifier_result = {'kpi_fk': kpi_fk}
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                       result=total_result, score=total_result,
                                       identifier_result=identifier_result)

    def calculate_category_level(self):
        self.cat_lvl_res = self.atomic_kpi_results.groupby(['parent_kpi'], as_index=False).agg({'score_by_weight': np.sum})
        self.cat_lvl_res.rename(columns={'parent_kpi': 'kpi_type', 'score_by_weight': 'cat_score'}, inplace=True)
        identifier_parent = {'kpi_fk': self.common.get_kpi_fk_by_kpi_type(self.TOTAL_UAE_SCORE)}
        for i, result in self.cat_lvl_res.iterrows():
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(result['kpi_type'])
            identifier_result = {'kpi_fk': kpi_fk}
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                           result=result['cat_score'], score=result['cat_score'],
                                           identifier_parent=identifier_parent, identifier_result=identifier_result)

    def calculate_atomics(self):
        store_atomics = self.get_store_atomic_kpi_parameters()
        self.build_tiers_for_atomics(store_atomics)
        if not store_atomics.empty:
            # Option 1: rearrange kpi order and then calculate
            # store_atomics.reset_index(inplace=True)
            reordered_index = self.reorder_kpis(store_atomics)
            self.restore_children(store_atomics)
            for i in reordered_index:
                row = store_atomics.iloc[i]
                self.calculate_atomic_results(row)

            # Option 2: check for existence of children
            # for i, row in store_atomics:
            #     if row[self.CHILD_KPI]:
            #         child_kpis = row[self.CHILD_KPI] if isinstance(row[self.CHILD_KPI], (list, tuple)) \
            #                                                                     else [row[self.CHILD_KPI]]
            #         for kpi in child_kpis:
            #             ind = store_atomics[store_atomics[self.KPI_TYPE] == kpi].index[0]
            #             child_row = store_atomics.iloc[ind]
            #             self.calculate_atomic_results(child_row)
            #     else:
            #         self.calculate_atomic_results(row)


                # if row[self.KPI_TYPE] not in self.atomic_kpi_results['kpi_name'].values.tolist():
                #     kpi_type = row[self.KPI_FAMILY]
                #     self.atomic_function[kpi_type](row)

    def reorder_kpis(self, store_atomics):
        input_df = store_atomics.copy()
        # input_df = store_atomics[['pk', self.KPI_TYPE, self.CHILD_KPI]]
        input_df['remaining_child'] = input_df[self.CHILD_KPI].copy()
        input_df.loc[~(input_df['remaining_child'] == ''), 'remaining_child'] = input_df['remaining_child'].apply(
            lambda x: x if isinstance(x, list) else [x])
        reordered_df = pd.DataFrame(columns=input_df.columns.values.tolist())
        child_flag = True
        while child_flag:
            input_df['remaining_child'] = input_df.apply(self.check_remaining_child, axis=1, args=(input_df,))
            remaining_df = input_df[(input_df['remaining_child'] == '') |
                                    (input_df['remaining_child'].isnull())]
            reordered_df = reordered_df.append(remaining_df)
            input_df = input_df[(~(input_df['remaining_child'] == '')) &
                                (~(input_df['remaining_child'].isnull()))]
            if input_df.empty:
                child_flag = False
        # reordered_pks = reordered_df['pk'].values.tolist()
        reordered_index = reordered_df.index
        return reordered_index

    def check_remaining_child(self, row, initial_df):
        child_kpis = row['remaining_child']
        if child_kpis:
            child_kpis = child_kpis if isinstance(child_kpis, (list, tuple)) else [child_kpis]
            for kpi in child_kpis:
                if kpi not in initial_df[self.KPI_TYPE].values:
                    ind_to_remove = [i for i, x in enumerate(child_kpis) if x == kpi]
                    for ind in ind_to_remove:
                        child_kpis.pop(ind)
                        if len(child_kpis) == 0:
                            child_kpis = ''
        return child_kpis

    def restore_children(self, store_atomics):
        parents_list = store_atomics[self.PARENT_KPI].unique().tolist()
        parents_list = filter(lambda x: x, parents_list)
        for parent in parents_list:
            child_kpis = store_atomics[store_atomics[self.PARENT_KPI] == parent][self.KPI_TYPE].unique().tolist()
            # store_atomics[self.CHILD_KPI] = store_atomics[self.CHILD_KPI].astype(object)
            store_atomics.loc[store_atomics[self.KPI_TYPE] == parent, self.CHILD_KPI] = [', '.join(child_kpis)]
            store_atomics[self.CHILD_KPI] = store_atomics[self.CHILD_KPI].apply(lambda x: x.split(',') if x else '')
            # store_atomics.at[store_atomics[self.KPI_TYPE] == parent, self.CHILD_KPI] = child_kpis

    def calculate_atomic_results(self, param_row):
        if param_row[self.KPI_TYPE] not in self.atomic_kpi_results[self.KPI_TYPE].values.tolist():
            kpi_type = param_row[self.KPI_FAMILY]
            self.atomic_function[kpi_type](param_row)

    def calculate_availability(self, param_row):
        if self.lvl3_assortment.empty:
            return
        lvl3_ass_res = self.lvl3_assortment[self.lvl3_assortment['ass_lvl2_kpi_type'] == param_row[self.KPI_TYPE]]
        if not lvl3_ass_res.empty:
            identifier_cat_parent = self.get_category_parent_dict(param_row)
            lvl3_ass_res = self.get_template_relevant_assortment_result(lvl3_ass_res, param_row)
            for i, row in lvl3_ass_res.iterrows():
                identifier_parent = {'kpi_fk': row.kpi_fk_lvl2}
                self.common.write_to_db_result(fk=row.kpi_fk_lvl3, numerator_id=row.product_fk,
                                               numerator_result=row.facings, denominator_result=1,
                                               denominator_id=row.product_fk, result=row.in_store,
                                               score=row.in_store, should_enter=True,
                                               identifier_parent=identifier_parent)

            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_ass_res)
            for i, row in lvl2_result.iterrows():
                identifier_result = {'kpi_fk': row.kpi_fk_lvl2}
                denominator_res = row.total
                if row.target and not np.math.isnan(row.target):
                    if row.group_target_date <= self.visit_date:
                        denominator_res = row.target
                result = np.divide(float(row.passes), float(denominator_res))
                score, weight = self.get_score(result, param_row)
                target = param_row[self.TARGET] if param_row[self.TARGET] else None
                self.common.write_to_db_result(fk=row.kpi_fk_lvl2, numerator_id=self.own_manuf_fk,
                                               numerator_result=row.passes, result=result * 100,
                                               denominator_id=self.store_id, denominator_result=denominator_res,
                                               score=score * weight, weight=weight, target=target,
                                               identifier_result=identifier_result,
                                               identifier_parent=identifier_cat_parent, should_enter=True)
                self.add_kpi_result_to_kpi_results_df([row.kpi_fk_lvl2, param_row[self.KPI_TYPE],
                                                       result, score, weight, score * weight,
                                                       param_row[self.KPI_LVL_2_NAME]])

    @staticmethod
    def add_actual_facings_to_assortment(lvl3_ass_res, scif):
        lvl3_ass_res['facings'] = 0
        product_assort = lvl3_ass_res['product_fk'].unique()
        for prod in product_assort:
            lvl3_ass_res.loc[lvl3_ass_res['product_fk'] == prod, 'facings'] = scif[scif['product_fk'] \
                                                                                   == prod]['facings'].sum()

    def get_template_relevant_assortment_result(self, lvl3_ass_res, param_row):
        ass_template = param_row[self.TEMPLATE_NAME_T]
        filtered_scif = self.scif[self.scif['template_name'] == ass_template]
        products_in_session = filtered_scif.loc[filtered_scif['facings'] > 0]['product_fk'].values
        lvl3_ass_res.loc[lvl3_ass_res['product_fk'].isin(products_in_session), 'in_store'] = 1
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
        relevant_step = min(tiers[tiers['step_value'] >= result]['step_value'].values.tolist())
        tier_score_value = tiers[tiers['step_value'] == relevant_step]['step_score_value'].values[0]
        score = tier_score_value
        return score

    def get_relative_score(self, param_row, result):
        target = float(param_row[self.TARGET])
        # think if maybe make the code error-friendly and to check float. ....
        score = result / target if target else 0
        return score

    def get_binary_score(self, param_row, result):
        target = float(param_row[self.TARGET])
        # think if maybe make the code error-friendly and to check float. ....
        score = 1 if result >= target else 0
        return score

    def calculate_linear_sos(self, param_row):
        general_filters = self.get_general_filters(param_row)
        sos_filters = self.get_sos_filters(param_row)
        if sos_filters is not None:
            numerator_filters = sos_filters['num_filters'].update(general_filters)
            denominator_filters = sos_filters['denom_filters'].update(general_filters)
            numerator_length = self.calculate_linear_space(numerator_filters)
            denominator_length = self.calculate_linear_space(denominator_filters)
            result = numerator_length / denominator_length if denominator_length else 0
            score, weight = self.get_score(result=result, param_row=param_row)

            num_id = param_row['param_value_1/numerator_value'] if \
                        isinstance(param_row['param_value_1/numerator_value'], (str, unicode)) else self.own_manuf_fk
            denom_id = param_row['param_value_2/denom_value']
            identifier_parent = self.get_identifier_parent_for_atomic(param_row)
            identifier_result = self.get_identifier_result_for_atomic(param_row)
            target = param_row[self.TARGET] if param_row[self.TARGET] else None
            self.common.write_to_db_result(fk=param_row['kpi_level_2_fk'], numerator_id=num_id,
                                           numerator_result=numerator_length, denominator_id=denom_id,
                                           denominator_result=denominator_length, target=target,
                                           result=result, score=score * weight, weight=weight,
                                           identifier_parent=identifier_parent, identifier_result=identifier_result,
                                           should_enter=True)
            self.add_kpi_result_to_kpi_results_df([param_row['kpi_level_2_fk'], param_row[self.KPI_TYPE],
                                                   result, score, weight, score * weight,
                                                   param_row[self.KPI_LVL_2_NAME]])

    def calculate_displays(self, param_row):
        general_filters = self.get_general_filters(param_row)
        filtered_scif = filter_df(general_filters, self.scif)
        result = len(filtered_scif['scene_fk'].unique().tolist())
        score, weight = self.get_score(result, param_row)
        identifier_parent = self.get_identifier_parent_for_atomic(param_row)
        #TODO: think!!  not sure I want to use identifier result here: I know that this kpi will not have children but I want to be as generic as possible
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
        if param_row[self.CHILD_KPI]:
            identifier_result = {'kpi': param_row['kpi_level_2_fk']}
        return identifier_result

    def calculate_checkouts(self, param_row):
        filters = self.get_general_filters(param_row)
        kpi_filters = self.get_non_sos_kpi_filters(param_row)
        filters.update(kpi_filters)
        filtered_matches = filter_df(filters, self.matches_products)
        filtered_matches = filtered_matches[filtered_matches['stacking_layer'] == 1]
        scene_probe_groups = filtered_matches.drop_duplicates(subset=['scene_fk', 'probe_group_id'])
        result = len(scene_probe_groups)
        score, weight = self.get_score(param_row=param_row, result=result)
        # maybe i should have a function that unifies these rows.
        target = param_row[self.TARGET] if param_row[self.TARGET] else None
        identifier_parent = self.get_identifier_parent_for_atomic(param_row)
        identifier_result = self.get_identifier_result_for_atomic(param_row)
        self.common.write_to_db_result(fk=param_row['kpi_level_2_fk'], numerator_id=self.own_manuf_fk,
                                       numerator_result=result, result=result, target=target,
                                       denominator_id=self.store_id, score=score * weight, weight=weight,
                                       identifier_parent=identifier_parent, identifier_result=identifier_result,
                                       should_enter=True)
        self.add_kpi_result_to_kpi_results_df([param_row['kpi_level_2_fk'], param_row['kpi_type'],
                                               result, score, weight, score * weight,
                                               param_row[self.KPI_LVL_2_NAME]])

    def calculate_block(self, param_row):
        if self.lvl3_assortment.empty:
            return
        block_ass = self.lvl3_assortment[self.lvl3_assortment['ass_lvl2_kpi_type'] == param_row[self.KPI_TYPE]]
        if not block_ass.empty:
            block_ass = self.get_template_relevant_assortment_result(block_ass, param_row)
            relevant_products = block_ass['product_fk'].unique().tolist()
            block_clusters = self.get_relevant_block_clusters(relevant_products, param_row)
            cluster_results = list()
            number_of_products = float(len(relevant_products))
            for cluster in block_clusters:
                facings = len(cluster.nodes['probe_matck_fk']) # verify how the data looks
                cluster_results.append(facings/number_of_products)
            result = max(cluster_results)
            score, weight = self.get_score(result, param_row)

            target = param_row[self.TARGET] if param_row[self.TARGET] else None
            identifier_parent = self.get_identifier_parent_for_atomic(param_row)
            identifier_result = self.get_identifier_result_for_atomic(param_row)
            self.common.write_to_db_result(fk=param_row['kpi_level_2_fk'], numerator_id=self.own_manuf_fk,
                                           numerator_result=result, result=result, target=target,
                                           denominator_id=self.store_id, score=score * weight, weight=weight,
                                           identifier_parent=identifier_parent, identifier_result=identifier_result,
                                           should_enter=True)
            self.add_kpi_result_to_kpi_results_df([param_row['kpi_level_2_fk'], param_row[self.KPI_TYPE],
                                                   result, score, weight, score * weight,
                                                   param_row[self.KPI_LVL_2_NAME]])

    def get_relevant_block_clusters(self, relevant_products, param_row):
        general_filters = self.get_general_filters(param_row)
        scenes = general_filters['location']['scene_fk']
        block_filters = {'product_fk': relevant_products}
        additional_filters = {'minimum_facing_for_block': 2, 'minimum_block_ratio': 0}
        cluster_list = list()
        for scene in scenes:
            location_filters = {'scene_fk': [scene]}
            block_res = self.block.network_x_block_together(block_filters, location_filters, additional_filters)
            block_res = block_res[block_res['is_block']]
            if not block_res:
                continue
            else:
                max_ratio = block_res['facing_percentage'].max()
                largest_cluster = block_res[block_res['facing_percentage'] == max_ratio]['cluster'].values[0]
                cluster_list.append(largest_cluster)
        return cluster_list

    def calculate_kpi_combination_score(self, param_row):
        child_kpis = param_row[self.CHILD_KPI] if isinstance(param_row[self.CHILD_KPI], (list, tuple)) \
                                                    else [param_row[self.CHILD_KPI]]
        child_results = self.atomic_kpi_results[self.atomic_kpi_results['kpi_type'].isin(child_kpis)]
        result = len(child_results[(child_results['score'] == 1) | (child_results['score'] == 100)])
        score, weight = self.get_score(result, param_row)
        target = param_row[self.TARGET] if param_row[self.TARGET] else None
        identifier_parent = self.get_identifier_parent_for_atomic(param_row)
        identifier_result = self.get_identifier_result_for_atomic(param_row)
        self.common.write_to_db_result(fk=param_row['kpi_level_2_fk'], numerator_id=self.own_manuf_fk,
                                       result=result, target=target, denominator_id=self.store_id,
                                       score=score * weight, weight=weight,
                                       identifier_parent=identifier_parent, identifier_result=identifier_result,
                                       should_enter=True)
        self.add_kpi_result_to_kpi_results_df([param_row['kpi_level_2_fk'], param_row[self.KPI_TYPE],
                                               result, score, weight, score * weight,
                                               param_row[self.KPI_LVL_2_NAME]])

    def calculate_linear_space(self, filters):
        filtered_scif = filter_df(filters, self.scif)
        space_length = filtered_scif['gross_len_ign_stack'].sum()
        return float(space_length)