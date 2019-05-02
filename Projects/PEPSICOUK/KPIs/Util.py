import os

import collections

from Projects.PEPSICOUK.Utils.Fetcher import PEPSICOUK_Queries
from Trax.Apps.Services.KEngine.KEUnified.Singleton import KEngineSingleton
from Projects.PEPSICOUK.Utils.CommonToolBox import PEPSICOUKCommonToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
# from KPIUtils_v2.DB.Common import Common as CommonV1
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
import pandas as pd
from Trax.Utils.Logging.Logger import Log
import numpy as np

class PepsicoUtil:
    __metaclass__ = KEngineSingleton

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
    BRAND_SPACE_TO_SALES_INDEX = 'Brand Space to Sales Index'
    BRAND_SPACE_SOS_VS_TARGET = 'Brand Space SOS vs Target'
    SUB_BRAND_SPACE_TO_SALES_INDEX = 'Sub Brand Space to Sales Index'
    SUB_BRAND_SPACE_SOS_VS_TARGET = 'Sub Brand Space SOS vs Target'
    PEPSICO_SEGMENT_SPACE_TO_SALES_INDEX = 'PepsiCo Segment Space to Sales Index'
    PEPSICO_SEGMENT_SOS_VS_TARGET = 'PepsiCo Segment SOS vs Target'
    PEPSICO_SUB_SEGMENT_SPACE_TO_SALES_INDEX = 'PepsiCo Sub Segment Space to Sales Index'
    PEPSICO_SUB_SEGMENT_SOS_VS_TARGET = 'PepsiCo Sub Segment SOS vs Target'

    def __init__(self, output, data_provider):
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
        self.kpi_results_check = pd.DataFrame(columns=['kpi_fk', 'numerator', 'denominator', 'result', 'score'])

    @staticmethod
    def get_full_bay_and_positional_filters(parameters): # get a function from ccbza
        filters = {parameters['Parameter 1']: parameters['Value 1']}
        if parameters['Parameter 2']:
            filters.update({parameters['Parameter 2']: parameters['Value 2']})
        if parameters['Parameter 3']:
            filters.update({parameters['Parameter 3']: parameters['Value 3']})
        return filters

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

    @staticmethod
    def split_and_strip(value):
        return map(lambda x: x.strip(), str(value).split(','))

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

    def calculate_sos(self, sos_filters, **general_filters):
        numerator_linear = self.calculate_share_space(**dict(sos_filters, **general_filters))
        denominator_linear = self.calculate_share_space(**general_filters)
        return float(numerator_linear), float(denominator_linear)

    def calculate_share_space(self, **filters):
        filtered_scif = self.filtered_scif[self.toolbox.get_filter_condition(self.filtered_scif, **filters)]
        space_length = filtered_scif['updated_gross_length'].sum()
        return space_length

    def add_kpi_result_to_kpi_results_df(self, result_list):
        self.kpi_results_check.loc[len(self.kpi_results_check)] = result_list

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

    def get_lvl3_relevant_assortment_result(self):
        assortment_result = self.assortment.get_lvl3_relevant_ass()
        if assortment_result.empty:
            return assortment_result
        products_in_session = self.filtered_scif.loc[self.filtered_scif['facings'] > 0]['product_fk'].values
        assortment_result.loc[assortment_result['product_fk'].isin(products_in_session), 'in_store'] = 1
        return assortment_result