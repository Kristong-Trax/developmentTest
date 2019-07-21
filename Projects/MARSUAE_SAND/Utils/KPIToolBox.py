
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import json
import os

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
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
        self.atomic_kpi_results = pd.DataFrame(columns=['kpi_fk', 'result', 'score', 'weight', 'parent_name'])
        # self.kpi_results_queries = []

    def get_category_level_targets(self):
        category_params = self.all_targets_unpacked[self.all_targets_unpacked['operation_type'] == self.CATEGORY_LEVEL]
        # move to dict
        return category_params

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
        if output_targets.empty:
            Log.warning('KPI External Targets Results are empty')
        return output_targets

    def get_relevant_atomic_kpi_parameters(self):
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
        return relevant_atomic_params_df

    def get_masking_filter(self, row, column, store_att_value):
        if store_att_value in self.split_and_strip(row[column]):
            return True
        else:
            return False

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

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        score = 0
        self.calculate_atomics()
        self.calculate_category_level()
        self.calculate_total_score()
        self.calculate_price()
        return score

    def calculate_total_score(self):
        pass

    def calculate_category_level(self):
        pass

    def calculate_atomics(self):
        atomic_params = self.get_relevant_atomic_kpi_parameters()
        if not atomic_params.empty:
            pass
        pass

    def calculate_price(self):
        pass