
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
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
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

    # Template columns
    KPI_FAMILY = 'KPI Family'
    TARGET = 'Target'
    KPI_LVL_2_NAME = 'KPI Level 2 Name'
    KPI_LVL_3_NAME = 'KPI Level 3 Name'
    TEMPLATE_NAME_T = 'Template Name'
    TIERED = 'Tiered'
    RELATIVE_SCORE = 'Relative score'
    BINARY = 'Binary'
    SCORE_LOGIC = 'score_logic'
    WEIGHT = 'Weight'
    MARS = 'MARS GCC'

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
        self.external_targets = self.get_all_kpi_external_targets()
        self.all_targets_unpacked = self.unpack_all_external_targets()
        self.full_store_info = self.get_store_data_by_store_id()
        self.store_info_dict = self.full_store_info.to_dict('records')[0]
        self.category_params = self.get_category_level_targets()
        # self.kpi_category_dict = self.get_kpi_category_dict()
        self.atomic_function = self.map_atomic_type_to_function()
        self.score_function = self.map_score_logic_to_function()
        self.assortment = Assortment(self.data_provider, self.output)
        self.lvl3_assortment = self.get_lvl3_relevant_assortment()
        self.own_manuf_fk = self.all_products[self.all_products['manufacturer_name'] ==
                                              self.MARS]['manufacturer_fk'].values[0]
        self.atomic_kpi_results = pd.DataFrame(columns=['kpi_fk', 'result', 'score', 'weight', 'parent_name'])
        self.atomic_tiers_df = pd.DataFrame()
        # self.kpi_results_queries = []

    def get_lvl3_relevant_assortment(self):
        assortment_result = self.assortment.get_lvl3_relevant_ass()
        kpi_static_data = self.kpi_static_data[['pk', 'type']]
        assortment_result = assortment_result.merge(kpi_static_data, left_on='kpi_fk_lvl2', right_on='pk', how='left')
        assortment_result = assortment_result.drop(columns=['pk'])
        assortment_result.rename({'type': 'ass_lvl2_kpi_type'}, inplace=True)
        return assortment_result

    def get_category_level_targets(self):
        category_params = self.all_targets_unpacked[self.all_targets_unpacked['operation_type'] == self.CATEGORY_LEVEL]
        return category_params

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
        atomic_params = atomic_params.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk', 'kpi_type'
                                                              'key_json', 'data_json'], keep='last')
        relevant_atomic_params_df = pd.DataFrame(columns=atomic_params.columns.values.tolist())
        if not atomic_params.empty:
            policies_df = self.unpack_external_targets_json_fields_to_df(atomic_params, field_name='key_json')
            policy_columns = policies_df.columns.values.tolist()
            del policy_columns[policy_columns.index('pk')]

            for column in policy_columns:
                store_att_value = self.store_info_dict.get(column)
                mask = policies_df.apply(self.get_masking_filter, args=(column, store_att_value), axis=1)
                policies_df = policies_df[mask]

            atomic_params_pks = policies_df['pk'].values.tolist()
            relevant_atomic_params_df = atomic_params[atomic_params['pk'].isin(atomic_params_pks)]
            data_json_df = self.unpack_external_targets_json_fields_to_df(relevant_atomic_params_df, 'data_json')
            relevant_atomic_params_df = relevant_atomic_params_df.merge(data_json_df, on='pk', how='left')
            relevant_atomic_params_df[self.WEIGHT] = relevant_atomic_params_df[self.WEIGHT].apply(lambda x: float(x))
        return relevant_atomic_params_df

    def get_tiers_for_atomics(self, atomics_df):
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
                  self.POI: self.calculate_displays,
                  self.PENETRATION: self.calculate_checkouts,
                  self.BLOCK: self.calculate_block}
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

    def add_atomic_result_to_kpi_results_df(self, result_list):
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
            if row[column]:
                if column.startswith('score_cond_target_'):
                    condition_number = str(column.strip('score_cond_target_'))
                    matching_value_col = filter(lambda x: x == 'score_cond_score_'.format(condition_number),
                                                relevant_columns)
                    value_col = matching_value_col[0] if len(matching_value_col) > 0 else None
                    if value_col:
                        tier_dict = {self.KPI_LVL_3_NAME: row[self.KPI_LVL_3_NAME], 'step_value': row[column],
                                     'step_score_value': row(value_col)}
                        tier_dict_list.append(tier_dict)

#-------------------------------main calculation section-----------------------------------

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        score = 0
        self.calculate_atomics()
        self.calculate_category_level()
        self.calculate_total_score()
        return score

    def calculate_total_score(self):
        pass

    def calculate_category_level(self):
        pass

    def calculate_atomics(self):
        store_atomics = self.get_store_atomic_kpi_parameters()
        self.get_tiers_for_atomics(store_atomics)
        if not store_atomics.empty:
            for i, row in store_atomics:
                kpi_type = row[self.KPI_FAMILY]
                self.atomic_function[kpi_type](row)

    def calculate_availability(self, param_row):
        if self.lvl3_assortment.empty:
            return
        lvl3_ass_res = self.lvl3_assortment[self.lvl3_assortment['ass_lvl2_kpi_type'] == param_row[self.KPI_LVL_3_NAME]]
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
                result = np.divide(float(row.passes), float(denominator_res)) * 100
                score = self.get_score(result, param_row)
                self.common.write_to_db_result(fk=row.kpi_fk_lvl2, numerator_id=self.own_manuf_fk,
                                               numerator_result=row.passes, result=result,
                                               denominator_id=self.store_id, denominator_result=denominator_res,
                                               score=score, weight=param_row[self.WEIGHT],
                                               identifier_result=identifier_result,
                                               identifier_parent=identifier_cat_parent, should_enter=True)
                self.add_atomic_result_to_kpi_results_df([row.kpi_fk_lvl2, self.own_manuf_fk, self.store_id,
                                                          result, score])

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
            Log.error('Score logic for kpi {} is not supported for kpi. '
                      'Score set to zero'.format(param_row[self.KPI_LVL_3_NAME]))
            score = 0
        return score

    def get_tiered_score(self, param_row, result):
        pass

    def get_relative_score(self, param_row, result):
        target = float(param_row[self.TARGET])
        # think if maybe make the code error-friendly....
        score = result / target * param_row[self.WEIGHT] if target else 0
        return score

    def get_binary_score(self, param_row, result):
        pass

    def calculate_linear_sos(self, param_row):
        pass

    def calculate_displays(self, param_row):
        pass

    def calculate_checkouts(self, param_row):
        pass

    def calculate_block(self, param_row):
        pass


