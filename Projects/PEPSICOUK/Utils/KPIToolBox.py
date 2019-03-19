
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

    # SOS_CATEGORIES_LIST = ['CSN']
    # HERO_SKU_LINEAR_SPACE_SHARE = 'Hero SKU Linear Space Share'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common_v1 = CommonV1(self.data_provider)
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
        self.kpi_results_queries = []

        self.toolbox = GENERALToolBox(data_provider)
        self.custom_entities = self.get_custom_entity_data()
        self.on_display_products = self.get_on_display_products()
        self.exclusion_template = self.get_exclusion_template_data()
        self.set_scif_and_matches_for_all_kpis(self.scif, self.match_product_in_scene) # check that scif and matches really changed for data provider

        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.store_info = self.data_provider['store_info'] # not sure i need it
        self.full_store_info = self.ps_data.get_ps_store_info(self.store_info) # not sure i need it
        # self.kpi_external_targets = self.ps_data.get_kpi_external_targets(kpi_operation_types=self.OPERATION_TYPES) #option 1
        # self.kpi_external_targets = self.ps_data.get_kpi_external_targets() # option 2
        # option 3: customize parsing of external targets table
        self.external_targets = self.get_all_kpi_external_targets()

        self.assortment = Assortment(self.data_provider, self.output, common=self.common_v1)
        self.lvl3_ass_result = self.assortment.calculate_lvl3_assortment()

#------------------init functions-----------------
    def get_all_kpi_external_targets(self):
        query = PEPSICOUK_Queries.get_kpi_external_targets(self.visit_date)
        external_targets = pd.read_sql_query(query, self.rds_conn.db)
        # if not external_targets.empty:
        #     external_targets['key_json'] = external_targets['key_json'].apply(lambda x: json.loads(x))
        #     external_targets['data_json'] = external_targets['data_json'].apply(lambda x: json.loads(x))
        return external_targets

    def get_custom_entity_data(self):
        query = PEPSICOUK_Queries.get_custom_entities_query()
        custom_entity_data = pd.read_sql_query(query, self.rds_conn.db)
        return custom_entity_data

    def get_on_display_products(self):
        probe_match_list = self.match_product_in_scene['probe_match_fk'].values.tolist()
        query = PEPSICOUK_Queries.on_display_products_query(probe_match_list)
        on_display_products = pd.read_sql_query(query, self.rds_conn.db)
        return on_display_products

    def get_exclusion_template_data(self):
        excl_templ = pd.read_excel(self.EXCLUSION_TEMPLATE_PATH)
        excl_templ = excl_templ.fillna('')
        return excl_templ

    def set_scif_and_matches_for_all_kpis(self, scif, matches):
        excl_template_all_kpis = self.exclusion_template[self.exclusion_template['KPI'].str.upper() == 'ALL']
        if not excl_template_all_kpis.empty:
            template_filters = self.get_filters_dictionary(excl_template_all_kpis)
            scif, matches = self.filter_scif_and_matches_for_scene_and_product_filters(template_filters, scif, matches)
            scif, matches = self.update_scif_and_matches_for_smart_attributes(scif, matches)
            self.data_provider._set_scene_item_facts(scif)
            self.data_provider._set_matches(matches)

#------------------utility functions--------------
    def update_scif_and_matches_for_smart_attributes(self, scif, matches):
        matches = self.filter_matches_for_products_with_smart_attributes(matches)
        aggregated_matches = self.aggregate_matches(matches)
        # remove relevant products from scif
        scif = self.update_scif_for_products_with_smart_attributes(scif, aggregated_matches)
        return scif, matches

    def aggregate_matches(self, matches):
        matches = matches[~(matches['bay_number'] == -1)]
        # ask about oos equivalent in matches - should not exist for the project
        matches['facings_matches'] = 1
        aggregated_df = matches.groupby(['scene_fk', 'product_fk'], as_index=False).agg({'width_mm_advance': np.sum,
                                                                                         'facings_matches': np.sum})
        return aggregated_df

    def filter_matches_for_products_with_smart_attributes(self, matches):
        matches = matches.merge(self.on_display_products, on='probe_match_fk', how='left')
        matches = matches[~(matches['smart_attribute'] == self.ADDITIONAL_DISPLAY)]
        return matches

    @staticmethod
    def update_scif_for_products_with_smart_attributes(scif, agg_matches):
        scif = scif.merge(agg_matches, on=['scene_fk', 'product_fk'], how='left')
        scif = scif[~scif['facings_matches'].isnull()]
        scif.rename(columns={'width_mm_advance': 'updated_gross_length', 'facings_matches': 'updated_facings'},
                    inplace=True)
        return scif

    def get_filters_dictionary(self, excl_template_all_kpis):
        filters = {}
        for i, row in excl_template_all_kpis.iterrows():
            if row['Action'].upper() == 'INCLUDE':
                filters.update({row['Type']: self.split_and_strip(row['Value'])})
            if row['Action'].upper() == 'EXCLUDE':
                filters.update({row['Type']: (self.split_and_strip(row['Value']), 0)})
        return filters

    @staticmethod
    def split_and_strip(value):
        return map(lambda x: x.strip(), str(value).split(','))

    def get_filters_for_scif_and_matches(self, template_filters):
        product_keys = filter(lambda x: x in self.data_provider[Data.ALL_PRODUCTS].columns.values.tolist(),
                              template_filters.keys())
        scene_keys = filter(lambda x: x in self.data_provider[Data.ALL_TEMPLATES].columns.values.tolist(),
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
        all_products = self.data_provider.all_products
        product_fk_list = all_products[self.toolbox.get_filter_condition(all_products, **filters)]
        product_fk_list = product_fk_list['product_fk'].unique().tolist()
        return product_fk_list

    def get_scene_fk_from_filters(self, filters):
        scif_data = self.data_provider[Data.SCENE_ITEM_FACTS]
        scene_fk_list = scif_data[self.toolbox.get_filter_condition(scif_data, **filters)]
        scene_fk_list = scene_fk_list['scene_fk'].unique().tolist()
        return scene_fk_list

    def filter_scif_and_matches_for_scene_and_product_filters(self, template_filters, scif, matches):
        filters = self.get_filters_for_scif_and_matches(template_filters)
        scif = scif[self.toolbox.get_filter_condition(scif, **filters)]
        matches = matches[self.toolbox.get_filter_condition(matches, **filters)]
        return scif, matches

#------------------main project calculations------

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates and writes to DB the KPI results.
        """
        self.calculate_external_kpis()
        self.calculate_internal_kpis()

    def calculate_external_kpis(self):
        self.calculate_assortment()
        self.calculate_sos_vs_target_kpis()

        # self.calculate_linear_sos_hero_sku()
        # self.calculate_linear_sos_brand()
        # self.calculate_linear_sos_sub_brand()
        # self.calculate_linear_sos_segment()
        pass

    def calculate_assortment(self):
        self.assortment.main_assortment_calculation()
        #try first the generic function (also look up pepsicoru)
        # for result in self.lvl3_ass_result.itertuples():
        #     score = result.in_store * 100
        #     self.common_v1.write_to_db_result_new_tables(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
        #                                                  numerator_result=result.in_store, result=score,
        #                                                  denominator_id=result.assortment_group_fk,
        #                                                  denominator_result=1, score=score)
        #
        # if not self.lvl3_ass_result.empty:
        #     lvl2_result = self.assortment.calculate_lvl2_assortment(self.lvl3_ass_result)
        #     for result in lvl2_result.itertuples():
        #         denominator_res = result.total
        #         if result.target and not np.math.isnan(result.target):
        #             if result.group_target_date <= self.visit_date:
        #                 denominator_res = result.target
        #         res = np.divide(float(result.passes), float(denominator_res)) * 100
        #         if res >= 100:
        #             score = 100
        #         else:
        #             score = 0
        #         self.common_v1.write_to_db_result_new_tables(fk=result.kpi_fk_lvl2, numerator_id=result.assortment_group_fk,
        #                                                      numerator_result=result.passes, result=res,
        #                                                      denominator_id=result.assortment_super_group_fk,
        #                                                      denominator_result=denominator_res, score=score)

    def calculate_sos_vs_target_kpis(self):
        sos_targets = self.get_relevant_sos_vs_target_kpi_targets()

        all_products_columns = self.all_products.columns.values.tolist()
        sos_targets['numerator_id'] = sos_targets.apply(self.retrieve_relevant_item_pks, axis=1,
                                                        args=('numerator_type', 'numerator_value',
                                                              all_products_columns))
        sos_targets['denominator_id'] = sos_targets.apply(self.retrieve_relevant_item_pks, axis=1,
                                                          args=('denominator_type', 'denominator_value',
                                                                all_products_columns))
        # potentially add row with identifier parent dictionary
        for i, row in sos_targets.iterrows():
            general_filters = {row['denominator_type']: row['denominator_value']}
            sos_filters = {row['numerator_type']: row['numerator_value']}
            numerator_linear, denominator_linear = self.calculate_sos(sos_filters, **general_filters)
            result = numerator_linear/denominator_linear if denominator_linear != 0 else 0
            score = result/row['Target'] if row['Target'] else 0 # what should we have in else case???
            self.common.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                           numerator_result=numerator_linear, denominator_id=row.denominator_id,
                                           denominator_result=denominator_linear, result=result, score=score)
        #in kpi external targets json_key=store data
        #in json_value: num, denom, target
        #generic function: since kpi_fk exists in external targets table
        pass

    def get_relevant_sos_vs_target_kpi_targets(self):
        sos_vs_target_kpis = self.external_targets[self.external_targets['operation_type'] == self.SOS_VS_TARGET]
        sos_vs_target_kpis = sos_vs_target_kpis.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk',
                                                                        'key_json', 'data_json'])
        relevant_targets_df = pd.DataFrame(columns=sos_vs_target_kpis.columns.values.tolist())
        if not sos_vs_target_kpis.empty:
            policies_df = self.unpack_external_targets_json_fields_to_df(sos_vs_target_kpis, field_name='key_json')
            policy_columns = policies_df.columns.values.tolist()
            del policy_columns[policy_columns.index('pk')]
            store_dict = self.full_store_info.to_dict('records')[0]
            for column in policy_columns:
                store_att_value = store_dict.get(column)
                policies_df = policies_df[policies_df[column] == store_att_value]
            kpi_targets_pks = policies_df['pk'].values.tolist()
            relevant_targets_df = sos_vs_target_kpis[sos_vs_target_kpis['pk'].isin(kpi_targets_pks)]
            # relevant_targets_df = relevant_targets_df.merge(policies_df, on='pk', how='left') # see if i will need it in the code
            data_json_df = self.unpack_external_targets_json_fields_to_df(relevant_targets_df, 'data_json')
            relevant_targets_df = relevant_targets_df.merge(data_json_df, on='pk', how='left')
        return relevant_targets_df

        # policies_list = []
        # for row in sos_vs_target_kpis.itertuples():
        #     policy = json.loads(row.key_json)
        #     policy.update({'pk': row.pk})
        #     policies_list.append(policy)
        #
        # policies_df = pd.DataFrame(policies_list)

    def retrieve_relevant_item_pks(self, row, type_field_name, value_field_name, all_products_columns):
        try:
            if row[type_field_name] in all_products_columns:
                item_id = self.all_products[self.all_products[row[type_field_name]] ==
                                                                  row[value_field_name]][type_field_name].values[0]
            else:
                item_id = self.custom_entities[self.custom_entities['name'] == row[value_field_name]]['name'].values[0]
            # fk_field = '{}_fk'.format(row[type_field_name])
            # name_field = row[type_field_name].replace('_name', '')
            # if fk_field in all_products_columns:
            #     item_id = self.all_products[self.all_products[row[type_field_name]] == row[value_field_name]][fk_field].values[0]
            # elif name_field in all_products_columns:
            #     item_id = self.all_products[self.all_products[row[type_field_name]] == row[value_field_name]][name_field].values[0]
            # elif row[type_field_name].contains('product'):
            #     item_id = self.all_products[self.all_products[row[type_field_name]] == row[value_field_name]]['product_fk'].values[0]
            # else:
            #     item_id = self.custom_entities[self.custom_entities['name'] == row[value_field_name]].values[0]
        except KeyError as e:
            Log.error('No id found for field {}. Error: {}'.format(row[type_field_name], e))
            item_id = None
        return item_id

    @staticmethod
    def unpack_external_targets_json_fields_to_df(input_df, field_name):
        data_list = []
        for row in input_df.itertuples():
            data_item = json.loads(row[field_name])
            data_item.update({'pk': row.pk})
            data_list.append(data_item)
        output_df = pd.DataFrame(data_list)
        return output_df

    def calculate_linear_sos_hero_sku(self):
        # kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.HERO_SKU_LINEAR_SPACE_SHARE)
        # general_filters = {'category': self.SOS_CATEGORIES_LIST}
        pass

    def calculate_linear_sos_brand(self):
        pass

    def calculate_linear_sos_sub_brand(self):
        pass

    def calculate_linear_sos_segment(self):
        pass

    def calculate_sos(self, sos_filters, **general_filters):
        numerator_linear = self.calculate_share_space(**dict(sos_filters, **general_filters))
        denominator_linear = self.calculate_share_space(**general_filters)
        return numerator_linear, denominator_linear

    def calculate_share_space(self, **filters):
        filtered_scif = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        space_length = filtered_scif['updated_gross_length'].sum()
        return space_length

    def calculate_internal_kpis(self):
        pass
