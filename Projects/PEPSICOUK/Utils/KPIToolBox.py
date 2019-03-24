
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
    HERO_SKU_SPACE_TO_SALES_INDEX = 'Hero SKU Space to Sales Index'
    HERO_SKU_SOS_VS_TARGET = 'Hero SKU SOS vs Target'
    LINEAR_SOS_INDEX = 'Linear SOS Index'
    PEPSICO = 'PEPSICO'
    SHELF_PLACEMENT = 'Shelf Placement'
    HERO_SKU_PLACEMENT_TOP = 'Hero SKU Placement by shelf numbers_Top'
    # SENSATIONS_VS_KETTLE_INDEX = 'Sensations Greater Linear Space vs Kettle'
    # DORITOS_VS_PRINGLES_INDEX = 'Doritos Greater Linear Space vs Pringles'

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
        self.match_product_in_scene = self.data_provider[Data.MATCHES] # initial matches
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS] # initial scif
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.toolbox = GENERALToolBox(data_provider)
        self.custom_entities = self.get_custom_entity_data()
        self.on_display_products = self.get_on_display_products()
        self.exclusion_template = self.get_exclusion_template_data()
        self.filtered_scif = self.scif # filtered scif acording to exclusion template
        self.filtered_matches = self.match_product_in_scene # filtered scif according to exclusion template
        self.set_filtered_scif_and_matches_for_all_kpis(self.scif, self.match_product_in_scene) # also sets scif and matches in data provider

        self.scene_bay_shelf_product = self.get_facings_scene_bay_shelf_product()
        self.ps_data = PsDataProvider(self.data_provider, self.output) # which scif and matches do I need
        self.store_info = self.data_provider['store_info'] # not sure i need it
        self.full_store_info = self.ps_data.get_ps_store_info(self.store_info) # not sure i need it
        # self.kpi_external_targets = self.ps_data.get_kpi_external_targets(kpi_operation_types=self.OPERATION_TYPES) #option 1
        # self.kpi_external_targets = self.ps_data.get_kpi_external_targets() # option 2
        # option 3: customize parsing of external targets table
        self.external_targets = self.get_all_kpi_external_targets()

        self.assortment = Assortment(self.data_provider, self.output, common=self.common_v1)
        self.lvl3_ass_result = self.assortment.calculate_lvl3_assortment()
        self.own_manuf_fk = self.all_products[self.all_products['manufacturer_name'] == self.PEPSICO]['manufacturer_fk'].values[0]

#------------------init functions-----------------
    def get_facings_scene_bay_shelf_product(self):
        self.filtered_matches['count'] = 1
        aggregate_df = self.filtered_matches.groupby(['scene_fk', 'bay_number', 'shelf_number', 'product_fk'],
                                                     as_index=False).agg({'count': np.sum})
        return aggregate_df

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

    def set_filtered_scif_and_matches_for_all_kpis(self, scif, matches):
        excl_template_all_kpis = self.exclusion_template[self.exclusion_template['KPI'].str.upper() == 'ALL']
        if not excl_template_all_kpis.empty:
            template_filters = self.get_filters_dictionary(excl_template_all_kpis)
            scif, matches = self.filter_scif_and_matches_for_scene_and_product_filters(template_filters, scif, matches)
            scif, matches = self.update_scif_and_matches_for_smart_attributes(scif, matches)
            self.filtered_scif = scif
            self.filtered_matches = matches
            self.set_scif_and_matches_in_data_provider(scif, matches)

    def set_scif_and_matches_in_data_provider(self, scif, matches):
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
        # self.calculate_internal_kpis() # all on scene_level

    def calculate_external_kpis(self):
        self.calculate_assortment() # uses filtered scif
        self.calculate_sos_vs_target_kpis() # uses filtered scif
        self.calculate_linear_brand_vs_brand_index() # uses filtered scif
        self.calculate_shelf_placement_hero_skus() # uses both initial and filtered scif / matches

    def calculate_shelf_placement_hero_skus(self):
        shelf_placement_targets = self.get_shelf_placement_kpi_targets_data()
        if not shelf_placement_targets.empty:
            scene_bay_max_shelves = self.get_scene_bay_max_shelves(shelf_placement_targets)
            scene_bay_all_shelves = scene_bay_max_shelves.drop_duplicates(subset=['scene_fk', 'bay_number',
                                                                                  'shelves_all_placements'], keep='first')
            relevant_matches = self.filter_out_irrelevant_matches(scene_bay_all_shelves)
            for i, row in scene_bay_max_shelves:
                shelf_list = map(lambda x: float(x), row['Shelves From Bottom To Include (data)'].split(','))
                relevant_matches.loc[(relevant_matches['scene_fk'] == row['scene_fk']) &
                                     (relevant_matches['bay_number'] == row['bay_number']) &
                                     (relevant_matches['shelf_number'].isin(shelf_list)), 'position'] = row['type']
            hero_results = self.get_kpi_results_df(relevant_matches, scene_bay_max_shelves)
            for i, result in hero_results.iterrows():
                self.common.write_to_db_result(fk=result['kpi_level_2_fk'], numerator_id=result['product_fk'],
                                               denominator_id=result['product_fk'],
                                               denominator_result=result['total_facings'],
                                               numerator_result=result['count'], result=result['ratio'],
                                               score=result['ratio'], identifier_parent=result['identifier_parent'],
                                               should_enter=True)
            hero_parent_results = hero_results.drop_duplicates(subset=['product_fk'])
            hero_top_kpi = self.common.get_kpi_fk_by_kpi_type('Hero Placement')
            hero_top_kpi_identifier_parent = self.common.get_dictionary(kpi_fk=hero_top_kpi)
            for i, result in hero_parent_results.iterrows():
                hero_parent_fk = self.common.get_kpi_fk_by_kpi_type(result['KPI Parent'])
                self.common.write_to_db_result(fk=hero_parent_fk, result=100, numerator_id=result['product_fk'],
                                               score=100, identifier_result=result['identifier_parent'],
                                               identifier_parent=hero_top_kpi_identifier_parent,
                                               should_enter=True)
            # add result fpr all hero
            self.common.write_to_db_result(fk=hero_top_kpi, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                           identifier_result=hero_top_kpi_identifier_parent, should_enter=True,
                                           score=1) # maybe customize picture for score type

    def get_kpi_results_df(self, relevant_matches, kpi_targets_df):
        total_products_facings = relevant_matches.groupby(['product_fk'], as_index=False).agg({'count': np.sum})
        total_products_facings.rename(columns={'count': 'total_facings'})
        # result_df = pd.pivot_table(relevant_matches, index=['product_fk'], columns=['type'], values='count',
        #                            aggfunc=np.sum)
        # result_df = result_df.reset_index()
        result_df = relevant_matches.groupby(['product_fk', 'type'], as_index=False).agg({'count':np.sum})
        result_df.merge(total_products_facings, on='product_fk', how='left')

        kpis_df = kpi_targets_df.drop_duplicates(subset=['kpi_level_2_fk', 'type', 'KPI Parent'])
        result_df = result_df.merge(kpis_df, on='type', how='left')
        result_df['identifier_parent'] = result_df['KPI Parent'].apply(lambda x:
                                                                       self.common.get_dictionary(
                                                                       kpi_fk=int(float(x))))
        result_df['ratio'] = result_df.apply(self.get_sku_ratio, axis=1)
        # kpi_list = kpi_targets_df['type'].values.tolist()
        # for kpi in kpi_list:
        #     facings_column = '{}_facings'.format(kpi)
        #     result_df[facings_column] = result_df[kpi]
        #     kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi)
        #     result_df[kpi_fk] = result_df[facings_column] / result_df['total_facings']
        # # write results for hero skus
        hero_results = result_df[result_df['product_fk'].isin(self.lvl3_ass_result['product_fk'].values.tolist())]
        return hero_results

    def get_sku_ratio(self, row):
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
        return scene_bay_max_shelves

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
        index_targets = self.get_relevant_sos_index_kpi_targets_data()
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
            numerator_sos = num_num_linear/num_denom_linear if num_denom_linear else 0 # ToDO: what should it be if denom is 0

            denominator_sos_filters = {row['denominator_type']: row['denominator_value']}
            denom_num_linear, denom_denom_linear = self.calculate_sos(denominator_sos_filters, **general_filters)
            denominator_sos = denom_num_linear/denom_denom_linear if denom_denom_linear else 0 #TODo: what should it be if denom is 0

            index = numerator_sos / denominator_sos if denominator_sos else 0 # TODO: what should it be if denom is 0
            self.common.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                           numerator_result=numerator_sos, denominator_id=row.denominator_id,
                                           denominator_result=denominator_sos, result=index, score=index,
                                           identifier_parent=row.identifier_parent, should_enter=True)

        parent_kpis_df = index_targets[['KPI Parent', 'identifier_parent']].drop_duplicates().reset_index(drop=True)
        parent_kpis_df.rename({'identifier_parent': 'identifier_result'}, inplace=True)
        for i, row in parent_kpis_df.iterrows():
            self.common.write_to_db_result(fk=row['KPI Parent'], numerator_id=self.own_manuf_fk,
                                           denominator_id=self.store_id, score=1, # TODO: think of what might be the score (we can have score type)
                                           identifier_result=row.identifier_result, should_enter=True)

    def get_relevant_sos_index_kpi_targets_data(self):
        sos_vs_target_kpis = self.external_targets[self.external_targets['operation_type'] == self.SOS_VS_TARGET]
        sos_vs_target_kpis = sos_vs_target_kpis.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk',
                                                                        'key_json', 'data_json'])
        relevant_targets_df = pd.DataFrame(columns=sos_vs_target_kpis.columns.values.tolist())
        if not sos_vs_target_kpis.empty:
            data_json_df = self.unpack_external_targets_json_fields_to_df(sos_vs_target_kpis, 'data_json')
            data_json_df = data_json_df[data_json_df['KPI Parent'] == self.LINEAR_SOS_INDEX]
            kpi_targets_pks = data_json_df['pk'].values.tolist()
            relevant_targets_df = sos_vs_target_kpis[sos_vs_target_kpis['pk'].isin(kpi_targets_pks)]
            relevant_targets_df = relevant_targets_df.merge(data_json_df, on='pk', how='left')
        return relevant_targets_df

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
        sos_targets['identifier_parent'] = sos_targets['KPI Parent'].apply(lambda x:
                                                                           self.common.get_dictionary(kpi_fk=int(float(x))))
        for i, row in sos_targets.iterrows():
            general_filters = {row['denominator_type']: row['denominator_value']}
            sos_filters = {row['numerator_type']: row['numerator_value']}
            numerator_linear, denominator_linear = self.calculate_sos(sos_filters, **general_filters)
            result = numerator_linear/denominator_linear if denominator_linear != 0 else 0
            score = result/row['Target'] if row['Target'] else 0 # what should we have in else case???
            self.common.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                           numerator_result=numerator_linear, denominator_id=row.denominator_id,
                                           denominator_result=denominator_linear, result=result, score=score,
                                           identifier_parent=row.identifier_parent, should_enter=True)

        parent_kpis_df = sos_targets[['KPI Parent', 'identifier_parent']].drop_duplicates().reset_index(drop=True)
        parent_kpis_df.rename({'identifier_parent': 'identifier_result'}, inplace=True)
        for i, row in parent_kpis_df.iterrows():
            self.common.write_to_db_result(fk=row['KPI Parent'], score=1, should_enter=True,
                                           numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                           identifier_result=row.identifier_result) # think of what might be the score (we can have score type)

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

            kpi_data = self.kpi_static_data[self.kpi_static_data['delete_time'].isnull()][['pk', 'type']].drop_duplicates() # see if I need more columns
            relevant_targets_df = relevant_targets_df.merge(kpi_data, left_on='kpi_level_2_fk', right_on='pk', how='left')
        return relevant_targets_df

        # policies_list = []
        # for row in sos_vs_target_kpis.itertuples():
        #     policy = json.loads(row.key_json)
        #     policy.update({'pk': row.pk})
        #     policies_list.append(policy)
        #
        # policies_df = pd.DataFrame(policies_list)

    def retrieve_relevant_item_pks(self, row, type_field_name, value_field_name):
        try:
            if row[type_field_name].endswith("_fk"):
                item_id = self.all_products[self.all_products[row[type_field_name]] ==
                                                                 row[value_field_name]][type_field_name].values[0]
            else:
                item_id = self.custom_entities[self.custom_entities['name'] == row[value_field_name]]['pk'].values[0]
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

    def calculate_sos(self, sos_filters, **general_filters):
        numerator_linear = self.calculate_share_space(**dict(sos_filters, **general_filters))
        denominator_linear = self.calculate_share_space(**general_filters)
        return numerator_linear, denominator_linear

    def calculate_share_space(self, **filters):
        filtered_scif = self.filtered_scif[self.toolbox.get_filter_condition(self.filtered_scif, **filters)]
        space_length = filtered_scif['updated_gross_length'].sum()
        return space_length

    def calculate_internal_kpis(self):
        pass
