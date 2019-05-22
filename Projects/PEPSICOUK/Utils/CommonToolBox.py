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
                                           'Inclusion_Exclusion_Template.xlsx')
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
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.full_store_info = self.get_store_data_by_store_id()
        self.store_info_dict = self.full_store_info.to_dict('records')[0]
        self.store_policy_exclusion_template = self.get_store_policy_data_for_exclusion_template()

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
        return scif, matches

    def get_kpi_type_by_pk(self, kpi_fk):
        try:
            kpi_fk = int(float(kpi_fk))
            return self.kpi_static_data[self.kpi_static_data['pk'] == kpi_fk]['type'].values[0]
        except IndexError:
            Log.info("Kpi name: {} is not equal to any kpi name in static table".format(kpi_fk))
            return None