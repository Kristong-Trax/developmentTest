# -*- coding: utf-8 -*-
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.PNGCN_PROD.ShareOfDisplay.ExcludeDataProvider import Fields
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import KPIUtils_v2.Utils.Parsers.ParseInputKPI as Parser
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block as Block
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime

__Author__ = 'Dudi_s and ilays'

CUBE = 'Cube'
NON_BRANDED_CUBE = 'Non branded cube'
CUBE_DISPLAYS = [CUBE, NON_BRANDED_CUBE]
CUBE_FK = 1
NON_BRANDED_CUBE_FK = 5
NEW_LSOS_KPI = 'LINEAR_GROSS_NSOS_IN_SCENE'
CUBE_TOTAL_DISPLAYS = ['Total 1 cube', 'Total 2 cubes', 'Total 3 cubes', 'Total 4 cubes', 'Total 5 cubes',
                       'Total 6 cubes', 'Total 7 cubes', 'Total 8 cubes', 'Total 9 cubes', 'Total 10 cubes',
                       'Total 11 cubes', 'Total 12 cubes', 'Total 13 cubes', 'Total 14 cubes', 'Total 15 cubes']
FOUR_SIDED = ['4 Sided Display']
FOUR_SIDED_FK = 31
FOUR_SIDED_TOTAL_DISPLAYS = ['Total 1 4-sided display', 'Total 2 4-sided display', 'Total 3 4-sided display',
                             'Total 4 4-sided display', 'Total 5 4-sided display']
PROMOTION_WALL_DISPLAYS = ['Product Strip']
TABLE_DISPLAYS = ['Table']
TABLE_TOTAL_DISPLAYS = ['Table Display']
DISPLAY_SIZE_KPI_NAME = 'DISPLAY_SIZE_PER_SKU_IN_SCENE'
PNG_MANUFACTURER = 'P&G宝洁'
DISPLAY_SIZE_PER_SCENE = 'DISPLAY_SIZE_PER_SCENE'
LINEAR_SOS_MANUFACTURER_IN_SCENE = 'LINEAR_SOS_MANUFACTURER_IN_SCENE'
PRESIZE_LINEAR_SOS_MANUFACTURER_IN_SCENE = 'PRESIZE_LINEAR_SOS_MANUFACTURER_IN_SCENE'

# EYE LEVEL KPIs
Eye_level_kpi_FACINGS = "Eye_level_kpi_FACINGS"
EYE_LEVEL_SE_BR_KPI = 'Eye_level_SEQUENCE_brand'
EYE_LEVEL_SE_BR_SB_KPI = 'Eye_level_SEQUENCE_brand_subbrand'
EYE_LEVEL_SE_BR_SC_KPI = 'Eye_level_SEQUENCE_brand_subcategory'
EYE_LEVEL_SE_BR_SC_SB_KPI = 'Eye_level_SEQUENCE_brand_subcategory_subbrand'
EYE_LEVEL_SE_BR_SB_FL_KPI = 'Eye_level_SEQUENCE_brand_subbrand_flavor'
EYE_LEVEL_SE_BR_SC_FL_KPI = 'Eye_level_SEQUENCE_brand_subcategory_flavor'
EYE_LEVEL_SE_BR_SC_SB_FL_KPI = 'Eye_level_SEQUENCE_brand_subcategory_subbrand_flavor'

EYE_LEVEL_GROUP_ATTRIBUTES = {
    EYE_LEVEL_SE_BR_KPI: {'group_level': ['brand_fk'], 'num_den_cont': ['brand_fk', 'store_fk', 'store_fk']},
    EYE_LEVEL_SE_BR_SB_KPI: {'group_level': ['brand_fk', 'sub_brand'],
                             'num_den_cont': ['sub_brand_fk', 'brand_fk', 'store_fk']},
    EYE_LEVEL_SE_BR_SC_KPI: {'group_level': ['brand_fk', 'sub_category_fk'],
                             'num_den_cont': ['sub_category_fk', 'brand_fk', 'store_fk']},
    EYE_LEVEL_SE_BR_SC_SB_KPI: {'group_level': ['brand_fk', 'sub_brand', 'sub_category_fk'],
                                'num_den_cont': ['sub_brand_fk', 'sub_category_fk', 'brand_fk']},
    EYE_LEVEL_SE_BR_SB_FL_KPI: {'group_level': ['brand_fk', 'sub_brand', 'att3'],
                                'num_den_cont': ['att3_fk', 'sub_brand_fk', 'brand_fk']},
    EYE_LEVEL_SE_BR_SC_FL_KPI: {'group_level': ['brand_fk', 'sub_category_fk', 'att3'],
                                'num_den_cont': ['att3_fk', 'sub_category_fk', 'brand_fk']},
    EYE_LEVEL_SE_BR_SC_SB_FL_KPI: {'group_level': ['brand_fk', 'sub_brand', 'sub_category_fk', 'att3'],
                                   'num_den_cont': ['att3_fk', 'sub_brand_fk', 'sub_category_fk']},
}
EYE_LEVEL_DUMMY_VALUE = ['brand_fk', 'brand_fk', 'brand_fk']

EYELEVEL_COLUMNS_TO_USE = ['bay_number', 'shelf_number', 'facing_sequence_number', 'stacking_layer',
                           'category_fk', 'brand_fk', 'sub_brand', 'sub_category_fk', 'att3']

# Variant block KPIs
BLOCK_BR_KPI = 'Block_Variant_brand'
BLOCK_BR_SB_KPI = 'Block_Variant_brand_subbrand'
BLOCK_BR_SC_KPI = 'Block_Variant_brand_subcategory'
BLOCK_BR_SC_SB_KPI = 'Block_Variant_brand_subcategory_subbrand'
BLOCK_BR_SB_FL_KPI = 'Block_Variant_brand_subbrand_flavor'
BLOCK_BR_SC_FL_KPI = 'Block_Variant_brand_subcategory_flavor'
BLOCK_BR_SC_SB_FL_KPI = 'Block_Variant_brand_subcategory_subbrand_flavor'
BLOCK_SKU = 'Block_Variant_SKU'

BLOCK_GROUP_ATTRIBUTES = {
    BLOCK_BR_KPI: {'group_level': ['brand_name'], 'num_den_cont': ['brand_fk', 'store_fk', 'store_fk']},
    BLOCK_BR_SB_KPI: {'group_level': ['brand_name', 'sub_brand'],
                      'num_den_cont': ['sub_brand_fk', 'brand_fk', 'store_fk']},
    BLOCK_BR_SC_KPI: {'group_level': ['brand_name', 'sub_category'],
                      'num_den_cont': ['sub_category_fk', 'brand_fk', 'store_fk']},
    BLOCK_BR_SC_SB_KPI: {'group_level': ['brand_name', 'sub_brand', 'sub_category'],
                         'num_den_cont': ['sub_brand_fk', 'sub_category_fk', 'brand_fk']},
    BLOCK_BR_SB_FL_KPI: {'group_level': ['brand_name', 'sub_brand', 'att3'],
                         'num_den_cont': ['att3_fk', 'sub_brand_fk', 'brand_fk']},
    BLOCK_BR_SC_FL_KPI: {'group_level': ['brand_name', 'sub_category', 'att3'],
                         'num_den_cont': ['att3_fk', 'sub_category_fk', 'brand_fk']},
    BLOCK_BR_SC_SB_FL_KPI: {'group_level': ['brand_name', 'sub_brand', 'sub_category', 'att3'],
                            'num_den_cont': ['att3_fk', 'sub_brand_fk', 'sub_category_fk', 'brand_fk']},
}
BLOCK_FIELDS = ['brand_name', 'sub_brand', 'sub_category', 'att3']
BLOCK_ATTRIBUTES = ['brand_fk', 'att3_fk', 'sub_brand_fk', 'sub_category_fk', 'category_fk']
BLOCK_DUMMY_VALUE = ['brand_name', 'brand_name', 'brand_name']
MIN_FACINGS_ON_SAME_LAYER = 'Min_facing_on_same_layer'
MIN_LAYER_NUMBER = 'Min_layer_#'
MATCH_PRODUCT_IN_PROBE_FK = 'match_product_in_probe_fk'
MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK = 'match_product_in_probe_state_reporting_fk'
MISSING_VALUE_FK = 999


class PngcnSceneKpis(object):

    def __init__(self, project_connector, common, scene_id, data_provider=None):
        self.scene_id = scene_id
        self.project_name = project_connector.project_name
        self.project_connector = project_connector
        if data_provider is not None:
            self.data_provider = data_provider
            self.on_ace = True
        else:
            self.on_ace = False
        self.cur = self.project_connector.db.cursor()
        self.log_prefix = 'Share_of_display for scene: {}, project {}'.format(self.scene_id,
                                                                              self.project_connector.project_name)
        Log.info(self.log_prefix + ' Starting calculation')
        self.match_display_in_scene = pd.DataFrame({})
        self.match_product_in_scene = pd.DataFrame({})
        self.displays = pd.DataFrame({})
        self.valid_facing_product = {}
        self.common = common
        self.matches_from_data_provider = self.data_provider[Data.MATCHES]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.location_type = "" if self.scif.empty else self.scif.iloc[0]['location_type']
        if self.location_type == 'Primary Shelf':
            self.eye_level_df = self.get_eye_level_shelves(self.matches_from_data_provider)
            eye_level_shelves = self.eye_level_df[['scene_match_fk', 'shelf_number']].copy()
            eye_level_shelves = eye_level_shelves.rename(columns={"shelf_number": "eye_level_shelf_number"})
            self.matches_from_data_provider = pd.merge(self.matches_from_data_provider, eye_level_shelves,
                                                       on='scene_match_fk', how="left")
        self.store_id = self.data_provider[Data.SESSION_INFO].store_fk.values[0]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.png_manufacturer_fk = self.get_png_manufacturer_fk()
        self.psdataprovider = PsDataProvider(data_provider=self.data_provider)
        self.parser = Parser
        self.match_probe_in_scene = self.get_product_special_attribute_data(self.scene_id)
        self.match_product_in_probe_state_reporting = self.psdataprovider.get_match_product_in_probe_state_reporting()
        self.sub_brand_entities = self.psdataprovider.get_custom_entities_df('sub_brand').drop_duplicates(
            subset=['entity_name'], keep='first')
        self.att3_entities = self.psdataprovider.get_custom_entities_df('att3').drop_duplicates(subset=['entity_name'],
                                                                                                keep='first')
        self.products = self.data_provider[Data.PRODUCTS]
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])

    def process_scene(self):
        if self.location_type == 'Primary Shelf':
            self.calculate_variant_block()
            self.save_nlsos_to_custom_scif()
            self.calculate_eye_level_kpi()
        self.calculate_linear_length()
        self.calculate_presize_linear_length()
        Log.debug(self.log_prefix + ' Retrieving data')
        self.match_display_in_scene = self._get_match_display_in_scene_data()
        # if there are no display tags there's no need to retrieve the rest of the data.
        if self.match_display_in_scene.empty:
            Log.debug(self.log_prefix + ' No display tags')
            self._delete_previous_data()
            self.calculate_display_size()
            self.common.commit_results_data(result_entity='scene')
        else:
            self.displays = self._get_displays_data()
            self.match_product_in_scene = self._get_match_product_in_scene_data()
            self._delete_previous_data()
            self._handle_promotion_wall_display()
            self._handle_cube_or_4_sided_display()
            self._handle_table_display()
            self._handle_rest_display()
            self.calculate_display_size()
            self.common.commit_results_data(result_entity='scene')
            if self.on_ace:
                Log.debug(self.log_prefix + ' Committing share of display calculations')
                self.project_connector.db.commit()
            Log.info(self.log_prefix + ' Finished calculation')

    @kpi_runtime()
    def calculate_variant_block(self, enable_single_shelf_exclusion=True):
        Log.info("Starting variant block KPI calculation")
        block_sku_kpi = self.common.get_kpi_fk_by_kpi_type(BLOCK_SKU)
        if self.matches_from_data_provider.empty or self.scif.empty or \
                self.scif.iloc[0]['location_type'] != 'Primary Shelf':
            return
        total_bays = self.matches_from_data_provider['bay_number'].max()
        block_results = {}
        kpi_aggrigations = {}
        full_df, custom_matches, products_df = self.get_full_df_and_products_df()
        self.save_highlight_products(custom_matches, kpi_level_2_type=BLOCK_BR_SB_KPI)
        irrelevant_products_fks = set(self.data_provider.all_products[self.data_provider.all_products['product_type']
                                                                      == 'Irrelevant']['product_fk'])
        self.data_provider.all_products.loc[self.data_provider.all_products['product_fk'].isin(
            irrelevant_products_fks), ['product_type']] = 'SKU'
        block_class = Block(self.data_provider, custom_matches=custom_matches)
        for grouping_kpi in BLOCK_GROUP_ATTRIBUTES.keys():
            grouping_field = grouping_kpi.replace('Block_Variant_', "")
            grouping_data = BLOCK_GROUP_ATTRIBUTES[grouping_kpi]
            group_attributes = grouping_data['num_den_cont']
            grouping_level = [grouping_field] + group_attributes
            kpi_aggrigations[grouping_kpi] = full_df.drop_duplicates(subset=set(grouping_level))[
                set(grouping_level)].to_dict(orient='records')
        for kpi_level in kpi_aggrigations.keys():
            filter_results = []
            kpi_block_fk = self.common.get_kpi_fk_by_kpi_type(kpi_level)
            filter_groups = kpi_aggrigations[kpi_level]
            conditions = {MIN_LAYER_NUMBER: 1, MIN_FACINGS_ON_SAME_LAYER: 2}
            for block_filters in filter_groups:
                block_attributes = self.build_block_attribute_dict(block_filters)
                filters = {k: [v] for k, v in block_filters.iteritems()}
                filter_block_result = block_class.network_x_block_together(
                    population=filters,
                    additional={'allowed_products_filters': {'product_type': ['Empty']},
                                'minimum_block_ratio': 0.0,
                                'minimum_facing_for_block': 2,
                                'include_stacking': False,
                                'check_vertical_horizontal': False})
                if filter_block_result.empty:
                    continue
                for j, row in filter_block_result.iterrows():
                    if not row['is_block']:
                        continue
                    scene_matches_fks = self.get_scene_match_fk(row)
                    block_df = self.parser.filter_df({"scene_match_fk": scene_matches_fks}, custom_matches)
                    shelves_df = block_df['shelf_number'].value_counts()
                    shelves_df_over_two_facings = block_df['shelf_number'].value_counts()[block_df['shelf_number'
                                                                                          ].value_counts() >= 2]
                    if enable_single_shelf_exclusion and (len(shelves_df) != len(shelves_df_over_two_facings)):
                        block_df_wo_stk = block_df[block_df['shelf_number'].isin(shelves_df_over_two_facings.index)]
                        relevant_scene_match_fks = block_df_wo_stk['scene_match_fk'].tolist()
                        filters['scene_match_fk'] = relevant_scene_match_fks
                        filter_block_result_new = block_class.network_x_block_together(
                            population=filters,
                            additional={'allowed_products_filters': {'product_type': ['Empty']},
                                        'minimum_block_ratio': 0.0,
                                        'minimum_facing_for_block': 2,
                                        'include_stacking': False,
                                        'check_vertical_horizontal': False})
                        del filters['scene_match_fk']
                        if filter_block_result_new.empty:
                            continue
                        for k, row_new in filter_block_result_new.iterrows():
                            if not row_new['is_block']:
                                continue
                            scene_matches_fks = self.get_scene_match_fk(row_new)
                            numerator_block = row_new['block_facings']
                            filters['stacking_layer'] = [1]
                            denominator_block = len(self.parser.filter_df(filters, custom_matches))
                            del filters['stacking_layer']
                            result_block = 0 if (denominator_block == 0) else numerator_block / float(denominator_block)
                            row_new['facing_percentage'] = result_block
                            row_new['SKU_ATTRIBUTES'] = block_attributes
                            row_new['kpi_level_2_fk'] = kpi_block_fk
                            self.handle_node_in_variant_block(conditions, row_new, scene_matches_fks, filter_results,
                                                              block_filters, custom_matches)
                    else:
                        row['SKU_ATTRIBUTES'] = block_attributes
                        row['kpi_level_2_fk'] = kpi_block_fk
                        self.handle_node_in_variant_block(conditions, row, scene_matches_fks, filter_results,
                                                          block_filters, custom_matches)
            block_results[kpi_level] = filter_results

        # Restore the original data provider
        self.data_provider.all_products.loc[self.data_provider.all_products['product_fk'].isin(
            irrelevant_products_fks), ['product_type']] = 'Irrelevant'

        # Save all blocks results
        for kpi in block_results.keys():
            kpi_attributes = BLOCK_GROUP_ATTRIBUTES[kpi]['num_den_cont']
            for row in block_results[kpi]:
                sku_attributes = row['SKU_ATTRIBUTES']
                brand_fk = sku_attributes.get("brand_fk")
                category_fk = row["category_fk"]
                number_of_max_level_facings = row['number_of_max_level_facings']
                number_of_min_level_facings = row['number_of_min_level_facings']
                numerator_id, denominator_id, context_id = self.get_kpi_attributes(kpi_attributes, sku_attributes)
                block_variant_kpi_fk = row['kpi_level_2_fk']
                block_eye_level_shelves = row['block_eye_level_shelves']
                identifier_result = str(sku_attributes) + "{}_{}_{}".format(row['bay_number'], row['vertical_location'],
                                                                            row['horizontal_location'])
                self.common.write_to_db_result(fk=block_variant_kpi_fk, numerator_id=numerator_id,
                                               denominator_id=denominator_id, context_id=context_id,
                                               numerator_result=row['horizontal_location'],
                                               denominator_result=row['vertical_location'],
                                               result=row['facing_percentage'], score=total_bays,
                                               weight=row['shelf_count'], target=row['bay_number'],
                                               by_scene=True, identifier_result=identifier_result)
                # Save all SKUs atomics per block
                products_data = row['SKU_DATA']
                for i, product_row in products_data.iterrows():
                    product_fk = product_row['product_fk']
                    facings_per_sku = product_row['facings_per_sku']
                    facings_ignore_stacking = product_row['facings_per_sku_ign_stack']
                    self.common.write_to_db_result(fk=block_sku_kpi, denominator_id=block_variant_kpi_fk,
                                                   numerator_id=product_fk, context_id=brand_fk,
                                                   result=facings_per_sku, target=facings_ignore_stacking,
                                                   score=category_fk, weight=block_eye_level_shelves,
                                                   numerator_result=number_of_max_level_facings,
                                                   denominator_result=number_of_min_level_facings,
                                                   by_scene=True, identifier_parent=identifier_result,
                                                   should_enter=True)

    @staticmethod
    def get_scene_match_fk(row):
        cluster = row['cluster']
        scene_matches_fks = []
        for node in cluster.nodes.data():
            scene_matches_fks += (list(node[1]['scene_match_fk']))
        return scene_matches_fks

    @staticmethod
    def get_kpi_attributes(kpi_attributes, sku_attributes):
        numerator_id = sku_attributes.get(kpi_attributes[0])
        denominator_id = sku_attributes.get(kpi_attributes[1])
        context_id = sku_attributes.get(kpi_attributes[2])
        return numerator_id, denominator_id, context_id

    @staticmethod
    def build_block_attribute_dict(block_filters):
        block_attributes_dict = {}
        for key in set(BLOCK_ATTRIBUTES + ['store_fk']).intersection(block_filters.keys()):
            value = block_filters.pop(key)
            block_attributes_dict[key] = value
        return block_attributes_dict

    def get_full_df_and_products_df(self):
        # Creating a custom self.products df
        products_df = self.products.copy()
        products_df[['brand_fk', 'sub_category_fk']] = products_df[['brand_fk', 'sub_category_fk']].fillna(
            MISSING_VALUE_FK)
        products_df = products_df.fillna("No Value")
        products_df['sub_brand_fk'] = products_df.merge(self.sub_brand_entities, left_on="sub_brand",
                                                        right_on="entity_name", how="left")['entity_fk']
        products_df['att3_fk'] = products_df.merge(self.att3_entities, left_on="att3", right_on="entity_name",
                                                   how="left")['entity_fk']
        products_df = products_df[['product_fk'] + BLOCK_ATTRIBUTES + BLOCK_FIELDS]

        # Creating a custom scif df
        full_df = self.scif.copy()
        full_df['store_fk'] = self.store_id
        full_df[['brand_fk', 'sub_category_fk']] = full_df[['brand_fk', 'sub_category_fk']].fillna(MISSING_VALUE_FK)
        full_df = full_df.fillna("No Value")
        full_df['sub_brand_fk'] = full_df.merge(self.sub_brand_entities, left_on="sub_brand",
                                                right_on="entity_name", how="left")['entity_fk']
        full_df['att3_fk'] = full_df.merge(self.att3_entities, left_on="att3", right_on="entity_name",
                                           how="left")['entity_fk']

        # Creating a custom attributes field on scif
        full_df['brand'] = full_df['brand_name']
        full_df['brand_subbrand'] = full_df['brand_name'].str.cat(full_df['sub_brand'], sep="_")
        full_df['brand_subcategory'] = full_df['brand_name'].str.cat(
            full_df['sub_category'], sep="_")
        full_df['brand_subcategory_subbrand'] = full_df['brand_name'].str.cat(
            full_df['sub_category'], sep="_").str.cat(full_df['sub_brand'], sep="_")
        full_df['brand_subbrand_flavor'] = full_df['brand_name'].str.cat(
            full_df['sub_brand'], sep="_").str.cat(full_df['att3'], sep="_")
        full_df['brand_subcategory_flavor'] = full_df['brand_name'].str.cat(
            full_df['sub_category'], sep="_").str.cat(full_df['att3'], sep="_")
        full_df['brand_subcategory_subbrand_flavor'] = full_df['brand_name'].str.cat(
            full_df['sub_category'], sep="_").str.cat(full_df['sub_brand'], sep="_"
                                                      ).str.cat(full_df['att3'], sep="_")

        # Creating a custom attributes field on matches
        custom_matches = self.matches_from_data_provider.copy().fillna("No Value")
        custom_matches = pd.merge(custom_matches, products_df, on="product_fk", how="left")
        custom_matches['brand'] = custom_matches['brand_name']
        custom_matches['brand_subbrand'] = custom_matches['brand_name'].str.cat(custom_matches['sub_brand'], sep="_")
        custom_matches['brand_subcategory'] = custom_matches['brand_name'].str.cat(
            custom_matches['sub_category'], sep="_")
        custom_matches['brand_subcategory_subbrand'] = custom_matches['brand_name'].str.cat(
            custom_matches['sub_category'], sep="_").str.cat(custom_matches['sub_brand'], sep="_")
        custom_matches['brand_subbrand_flavor'] = custom_matches['brand_name'].str.cat(
            custom_matches['sub_brand'], sep="_").str.cat(custom_matches['att3'], sep="_")
        custom_matches['brand_subcategory_flavor'] = custom_matches['brand_name'].str.cat(
            custom_matches['sub_category'], sep="_").str.cat(custom_matches['att3'], sep="_")
        custom_matches['brand_subcategory_subbrand_flavor'] = custom_matches['brand_name'].str.cat(
            custom_matches['sub_category'], sep="_").str.cat(custom_matches['sub_brand'], sep="_"
                                                             ).str.cat(custom_matches['att3'], sep="_")
        return full_df, custom_matches, products_df

    def encode_dict(self, block_filters):
        block_filters = {k: unicode(v).encode("utf-8") for k, v in block_filters.iteritems()}
        return block_filters

    def handle_node_in_variant_block(self, row_in_template, row, scene_matches_fks, filter_results, block_filters,
                                     custom_matches):
        block_df_include_empties = custom_matches[custom_matches['scene_match_fk'].isin(scene_matches_fks)]
        try:
            block_df = self.parser.filter_df(block_filters, block_df_include_empties)
        except:
            block_filters = self.encode_dict(block_filters)
            block_df = self.parser.filter_df(block_filters, block_df_include_empties)
        shelves = set(block_df['shelf_number'])
        # filter blocks without the minimum shelves spreading number
        if len(shelves) < row_in_template[MIN_LAYER_NUMBER]:
            return

        block_flag = False
        for shelf in shelves:
            shelf_df = block_df[block_df['shelf_number'] == shelf]

            # filter blocks without the minimum number of facings on the same layer
            if len(shelf_df) >= row_in_template[MIN_FACINGS_ON_SAME_LAYER]:
                block_flag = True
                break
        if block_flag:
            # Handling SKUs
            number_of_max_level_facings = block_df["shelf_number"].value_counts().iloc[0]
            row['number_of_max_level_facings'] = number_of_max_level_facings

            number_of_min_level_facings = block_df["shelf_number"].value_counts().iloc[-1]
            row['number_of_min_level_facings'] = number_of_min_level_facings

            block_eye_level_values = set(block_df['eye_level_shelf_number'])
            row['block_eye_level_shelves'] = sum(x in block_eye_level_values for x in [1.0, 2.0])
            row['SKU_DATA'] = self.get_skus_data_from_block(block_df, custom_matches, block_filters)

            # get bottom left facings
            vertical_location = block_df['shelf_number'].max()
            row['vertical_location'] = vertical_location
            shelf_df = block_df[block_df['shelf_number'] == vertical_location]
            bay_number = shelf_df['bay_number'].min()
            row['bay_number'] = bay_number
            bay_df = shelf_df[shelf_df['bay_number'] == bay_number]
            horizontal_location = bay_df['facing_sequence_number'].min()
            row['horizontal_location'] = horizontal_location
            row['shelf_count'] = len(shelves)

            # Adding left bottom corner category for client
            row['category_fk'] = bay_df[bay_df['facing_sequence_number'] ==
                                        horizontal_location]['category_fk'].values[0]
            filter_results.append(row)

    def get_skus_data_from_block(self, block_df, custom_matches, block_filters):
        facings_ign_stack_per_sku_df = block_df['product_fk'].reset_index().groupby("product_fk").count().reset_index()
        facings_ign_stack_per_sku_df.columns = ['product_fk', 'facings_per_sku_ign_stack']
        fields_to_filter_by = ['scene_fk', 'bay_number', 'shelf_number', 'facing_sequence_number']
        complete_df = block_df[fields_to_filter_by].merge(custom_matches, on=fields_to_filter_by)
        complete_df = self.parser.filter_df(block_filters, complete_df)
        facings_per_sku_df = complete_df['product_fk'].reset_index().groupby("product_fk").count().reset_index()
        facings_per_sku_df.columns = ['product_fk', 'facings_per_sku']
        sku_df = pd.merge(facings_ign_stack_per_sku_df, facings_per_sku_df, on="product_fk")
        return sku_df

    def save_highlight_products(self, custom_matches, kpi_level_2_type):
        kpi_block_fk = self.common.get_kpi_fk_by_kpi_type(kpi_level_2_type)
        match_product_in_probe_state_reporting = self.match_product_in_probe_state_reporting[
            self.match_product_in_probe_state_reporting['kpi_level_2_fk'] == kpi_block_fk]
        sub_brands = set(custom_matches['sub_brand'])
        try:
            for sub_brand in sub_brands:
                if sub_brand.encode("utf8") not in match_product_in_probe_state_reporting['name'
                ].str.encode("utf8").to_list():
                    self.insert_sub_brand_into_probe_state_reporting(sub_brand.encode("utf8"), kpi_block_fk)
                sub_brand_pk = self.match_product_in_probe_state_reporting[
                    (self.match_product_in_probe_state_reporting['kpi_level_2_fk'] == kpi_block_fk) &
                    (self.match_product_in_probe_state_reporting['name'].str.encode("utf8") ==
                     sub_brand.encode("utf8"))]['match_product_in_probe_state_reporting_fk'].values[0]
                df_to_append = pd.DataFrame(
                    columns=[MATCH_PRODUCT_IN_PROBE_FK, MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK])
                sub_brand_df = custom_matches[custom_matches['sub_brand'].str.encode("utf8") ==
                                              sub_brand.encode("utf8")]
                df_to_append[MATCH_PRODUCT_IN_PROBE_FK] = sub_brand_df['probe_match_fk'].drop_duplicates()
                df_to_append[MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK] = sub_brand_pk
                self.common.match_product_in_probe_state_values = \
                    self.common.match_product_in_probe_state_values.append(df_to_append)
        except Exception as ex:
            Log.error("Scene {} failed to write to highlight kpi, error: {}".format(str(self.scene_id), ex))

    def insert_sub_brand_into_probe_state_reporting(self, sub_brand, kpi_level_2_fk):
        query = """INSERT into static.match_product_in_probe_state_reporting (name, display_name, kpi_level_2_fk) \
                 VALUES ('{0}', '{0}', {1});""".format(sub_brand, kpi_level_2_fk)
        self.common.execute_custom_query(query)
        self.match_product_in_probe_state_reporting = self.psdataprovider.get_match_product_in_probe_state_reporting()

    def get_attribute_fk_from_name(self, name, value):
        try:
            if name == 'brand_name':
                attribute_fk = self.scif[self.scif[name] == value[0]]['brand_fk'].values[0]
            elif name == 'category':
                attribute_fk = self.scif[self.scif[name] == value[0]]['category_fk'].values[0]
            elif name == 'sub_brand_name':
                attribute_fk = self.get_custom_entity_fk(name, value[0]).values[0]
            else:
                attribute_fk = -1
        except Exception as ex:
            Log.warning("No attribute name: " + name + ", ERROR: ".format(ex))
            attribute_fk = -1
        return attribute_fk

    def get_custom_entity_fk(self, name, value):
        if name == 'sub_brand_name':
            attributes = self.sub_brand_entities
        else:
            return -1
        if attributes.empty:
            return -1
        attribute_fk = attributes[attributes['entity_name'].str.encode(
            "utf8") == value]['entity_fk']
        return attribute_fk

    @staticmethod
    def replace_with_seq_order(sorted_items, field):
        seq = 1
        for item in sorted_items:
            item["seq_" + field] = seq
            seq += 1

    @kpi_runtime()
    def calculate_eye_level_kpi(self):
        """
        calls the filter eyelevel shelves function, calls both eye_level_sequence and eye_level_facings KPIs
        """
        if self.matches_from_data_provider.empty or self.scif.empty or \
                self.scif.iloc[0]['location_type'] != 'Primary Shelf':
            return
        eye_level_df = self.get_eye_level_shelves(self.matches_from_data_provider)
        full_eye_level_df = pd.merge(eye_level_df, self.all_products, on="product_fk")
        max_shelf_count = self.matches_from_data_provider["shelf_number"].max()
        self.calculate_facing_eye_level(full_eye_level_df, max_shelf_count)
        self.calculate_sequence_eye_level(max_shelf_count, full_eye_level_df)

    def _get_custom_entities(self):
        eye_level_fragments = self.psdataprovider.get_custom_entities_df('eye_level_fragments')
        return eye_level_fragments

    def calculate_facing_eye_level(self, full_df, max_shelf_count):
        """
        Summing all facings for each product (includes stackings)
        :param full_df: the two relevant shelves (eye level shelves)
        :param max_shelf_count: the count of total shelf layer count (max value in all bays)
        :return: save the facing_eye_level results for each shelf (combine all bays)
        """
        kpi_facings_fk = self.common.get_kpi_fk_by_kpi_name(Eye_level_kpi_FACINGS)
        results_facings_df = full_df.groupby(
            by=['shelf_number', 'product_fk']).first().reset_index()
        summed_result_df = full_df.groupby(by=['shelf_number', 'product_fk']).size().reset_index()
        results_facings_df['result'] = summed_result_df[0].copy()
        for i, row in results_facings_df.iterrows():
            product_fk = row['product_fk']
            shelf_number = row['shelf_number']
            category_fk = row['category_fk']
            facings = row['result']
            self.common.write_to_db_result(fk=kpi_facings_fk, numerator_id=product_fk,
                                           denominator_id=category_fk, numerator_result=shelf_number,
                                           result=facings, score=max_shelf_count, by_scene=True)

    def calculate_sequence_eye_level(self, max_shelf_count, full_df):
        """
        Saving sequence of brand-sub_category blocks (not including stackings)
        :param entity_df: the sub_-category-brand custom_entety fields, to save the correct entity
        :param full_df: The df to work on
        :param scene_category: scene category to get the specific category filter
        :return: saves the sequence of each shelf (combine all bays)
        """
        results_sequence_df = pd.DataFrame(columns=['fk', 'numerator_id', 'denominator_id', 'context_id',
                                                    'numerator_result', 'result', 'score', 'by_scene'])
        full_df = full_df[EYELEVEL_COLUMNS_TO_USE]
        full_df = full_df[full_df['stacking_layer'] == 1]
        full_df[['att3', 'sub_brand']] = full_df[['att3', 'sub_brand']].fillna("No Value")
        full_df = full_df.fillna("-1")
        full_df = full_df.sort_values(["shelf_number", "bay_number", "facing_sequence_number"]).reset_index()
        full_df['store_fk'] = self.store_id
        full_df['sub_brand_fk'] = full_df.merge(self.sub_brand_entities, left_on="sub_brand",
                                                right_on="entity_name", how="left")['entity_fk']
        full_df['att3_fk'] = full_df.merge(self.att3_entities, left_on="att3", right_on="entity_name",
                                           how="left")['entity_fk']
        for grouping_kpi in EYE_LEVEL_GROUP_ATTRIBUTES.keys():
            frag_df = full_df.copy()
            kpi_sequence_fk = self.common.get_kpi_fk_by_kpi_type(grouping_kpi)
            grouping_data = EYE_LEVEL_GROUP_ATTRIBUTES[grouping_kpi]
            grouping_level = grouping_data['group_level'] + EYE_LEVEL_DUMMY_VALUE

            frag_df['group_order'] = ((frag_df['brand_fk'] != frag_df['brand_fk'].shift()) |
                                      (frag_df['shelf_number'] != frag_df['shelf_number'].shift()) |
                                      (frag_df[grouping_level[1]] != frag_df[grouping_level[1]].shift()) |
                                      (frag_df[grouping_level[2]] != frag_df[grouping_level[2]].shift()) |
                                      (frag_df[grouping_level[3]] != frag_df[grouping_level[3]].shift())).cumsum()

            facing_sequence_number = 1
            facings = frag_df.groupby('group_order', as_index=False).size().reset_index(name='facings')
            frag_df = frag_df.merge(facings, on="group_order")
            group_df = frag_df.drop_duplicates(subset=["group_order"], keep="first")
            max_sequence_shelf_1 = len(group_df[group_df['shelf_number'] == 1])
            for i, row in group_df.iterrows():
                numerator_id, denominator_id, context_id = row[grouping_data['num_den_cont']]
                shelf_number = row['shelf_number']
                score = row['facings']
                category_fk = row['category_fk']
                brand_fk = row['brand_fk']
                results_sequence_df = results_sequence_df.append({'fk': kpi_sequence_fk, 'numerator_id': numerator_id,
                                                                  'denominator_id': denominator_id, 'weight': brand_fk,
                                                                  'context_id': context_id, 'target': category_fk,
                                                                  'numerator_result': shelf_number,
                                                                  'denominator_result': max_shelf_count,
                                                                  'result': facing_sequence_number, 'score': score,
                                                                  'by_scene': True}, ignore_index=True)
                if (row['shelf_number'] == 1) and (facing_sequence_number == max_sequence_shelf_1):
                    facing_sequence_number = 0
                facing_sequence_number += 1
        # saving all results
        for i, row in results_sequence_df.iterrows():
            self.common.write_to_db_result(**row)

    @staticmethod
    def get_eye_level_shelves(df):
        """
        Gives us the two relevant shelves according to the costumer request.
        :param df: the df to work on
        :return: the two relevant eye_level shelves out of the df given
        """
        if df.empty:
            return df
        bay_and_shelves = df.groupby(by=['bay_number', 'shelf_number']).first().reset_index()[
            ['bay_number', 'shelf_number']]
        max_shelves = bay_and_shelves.groupby('bay_number').max().reset_index()
        bays_df = []
        for i, bays_data in max_shelves.iterrows():
            bay_number = bays_data['bay_number']
            highest_shelf = bays_data['shelf_number']
            if highest_shelf <= 6:
                shelves_to_choose = [1, 2]
            elif highest_shelf == 7:
                shelves_to_choose = [2, 3]
            elif highest_shelf == 8:
                shelves_to_choose = [3, 4]
            else:
                shelves_to_choose = [4, 5]
            bay_df = df[(df['bay_number'] == bay_number) & (
                df['shelf_number'].isin(shelves_to_choose))]
            final_bay_df = bay_df.copy()
            final_bay_df['shelf_number'] = bay_df['shelf_number'].map(
                {shelves_to_choose[0]: 1, shelves_to_choose[1]: 2})
            bays_df.append(final_bay_df)
        final_df = pd.concat(bays_df)
        return final_df

    def _handle_rest_display(self):
        """
        Handling all display tags which are not cubes and not promotion wall.
        Each tag in a scene is a display with a single bay.
        :return:
        """
        Log.debug(self.log_prefix + ' Starting non cube non promotion display')
        # filtering all rest displays tags
        display_non_cube_non_promotion_wall_with_bays = \
            self.match_display_in_scene[~self.match_display_in_scene['display_name'].isin(CUBE_DISPLAYS +
                                                                                          CUBE_TOTAL_DISPLAYS +
                                                                                          PROMOTION_WALL_DISPLAYS +
                                                                                          TABLE_DISPLAYS +
                                                                                          TABLE_TOTAL_DISPLAYS +
                                                                                          FOUR_SIDED +
                                                                                          FOUR_SIDED_TOTAL_DISPLAYS)]
        if not display_non_cube_non_promotion_wall_with_bays.empty:
            display_non_cube_non_promotion_wall_with_id_and_bays = \
                self._insert_into_display_surface(display_non_cube_non_promotion_wall_with_bays)
            # only valid tags are relevant
            display_non_cube_non_promotion_wall_with_id_and_valid_bays = \
                self._filter_valid_bays(display_non_cube_non_promotion_wall_with_id_and_bays)
            self._calculate_share_of_display(
                display_non_cube_non_promotion_wall_with_id_and_valid_bays)

    def _handle_promotion_wall_display(self):
        """
        Handles promotion wall display. All display tags in a scene are aggregated to one display with multiple bays
        :return:
        """
        Log.debug(self.log_prefix + ' Starting promotion display')
        promotion_tags = \
            self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(
                PROMOTION_WALL_DISPLAYS)]
        if not promotion_tags.empty:
            promotion_display_name = promotion_tags['display_name'].values[0]
            display_promotion_wall = promotion_tags.groupby(['display_fk', 'scene_fk'],
                                                            as_index=False).display_size.sum()
            display_promotion_wall['display_name'] = promotion_display_name
            display_promotion_wall_with_id = self._insert_into_display_surface(
                display_promotion_wall)
            promotion_wall_bays = promotion_tags[['scene_fk', 'bay_number']].copy()
            promotion_wall_bays.drop_duplicates(['scene_fk', 'bay_number'], inplace=True)
            # only valid tags are relevant
            promotion_wall_valid_bays = self._filter_valid_bays(promotion_wall_bays)
            display_promotion_wall_with_id_and_bays = \
                display_promotion_wall_with_id.merge(promotion_wall_valid_bays, on='scene_fk')
            self._calculate_share_of_display(display_promotion_wall_with_id_and_bays)

    def _handle_cube_or_4_sided_display(self):
        """
        Handles cube displays. All tags are aggregated to one display per scene with multiple tags.
        If there are cube/non branded cube tags the bays will be taken from them, otherwise from the 'total' tags.
        If there are 'total' tags the size will be calculated from them, otherwise from the cube/non branded cube tags
        :return:
        """
        Log.debug(self.log_prefix + ' Starting cube display')
        tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(
            CUBE_DISPLAYS + FOUR_SIDED)]
        total_tags = \
            self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(
                CUBE_TOTAL_DISPLAYS + FOUR_SIDED_TOTAL_DISPLAYS)]
        # remove mixed scenes with table and cube tags together
        table_tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(
            TABLE_DISPLAYS)]
        table_scenes = table_tags.scene_fk.tolist()
        scenes = tags.scene_fk.tolist() + total_tags.scene_fk.tolist()
        mixed_with_table_scenes = list(set(scenes) & set(table_scenes))
        scenes = list(set(scenes) - set(mixed_with_table_scenes))
        bays = pd.DataFrame({})
        display = pd.DataFrame({})
        for scene in scenes:
            tags_scene = tags[tags['scene_fk'] == scene]
            total_tags_scene = total_tags[total_tags['scene_fk'] == scene]
            if not (total_tags_scene.empty and tags_scene.empty):
                if total_tags_scene.empty:
                    bays_scene = tags_scene[['scene_fk', 'bay_number']].copy()
                    display_scene = tags_scene.groupby(
                        'scene_fk', as_index=False).display_size.sum()
                elif tags_scene.empty:
                    bays_scene = total_tags_scene[['scene_fk', 'bay_number']].copy()
                    display_scene = total_tags_scene.groupby(
                        'scene_fk', as_index=False).display_size.sum()
                else:
                    bays_scene = tags_scene[['scene_fk', 'bay_number']].copy()
                    display_scene = total_tags_scene.groupby(
                        'scene_fk', as_index=False).display_size.sum()
                # if there is even 1 cube tag in the scene, all display tags will be considered as cube
                if not (tags_scene[tags_scene['display_name'].isin(CUBE_DISPLAYS + CUBE_TOTAL_DISPLAYS)]).empty:
                    display_scene['display_fk'] = \
                        NON_BRANDED_CUBE_FK if (NON_BRANDED_CUBE in tags_scene['display_name'].values and
                                                CUBE not in tags_scene['display_name'].values) else CUBE_FK
                else:
                    display_scene['display_fk'] = FOUR_SIDED_FK

                display = display.append(display_scene, ignore_index=True)
                bays = bays.append(bays_scene, ignore_index=True)
                if display['display_fk'].values[0] == CUBE_FK:
                    display['display_name'] = CUBE
                elif display['display_fk'].values[0] == NON_BRANDED_CUBE_FK:
                    display['display_name'] = NON_BRANDED_CUBE
                else:
                    display['display_name'] = TABLE_DISPLAYS[0]

        if not bays.empty:
            bays.drop_duplicates(['scene_fk', 'bay_number'], inplace=True)
        if not display.empty:
            # only valid tags are relevant
            valid_bays = self._filter_valid_bays(bays)
            display_with_id = self._insert_into_display_surface(display)
            display_with_id_and_bays = display_with_id.merge(valid_bays, on=['scene_fk'])
            self._calculate_share_of_display(display_with_id_and_bays, all_skus=0)

    def _handle_table_display(self):
        """
            Handles table displays. All tags are aggregated to one display per scene with multiple tags.
            If there are cube/non branded cube tags also -> the display will be considered as Table display and cube size
            will be considered as 3 table size (3*table size) -> number of cubes will be determined by Total_CUBE tag,
            if there is no total_cube tag and only cube tags, we will ignore them and only table calculation will be applied
            :return:
            """
        Log.debug(self.log_prefix + ' Starting table display')
        table_tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(
            TABLE_DISPLAYS)]
        total_cube_tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(
            CUBE_TOTAL_DISPLAYS)]
        other_tags = \
            self.match_display_in_scene[~self.match_display_in_scene['display_name'].isin(TABLE_DISPLAYS +
                                                                                          TABLE_TOTAL_DISPLAYS + CUBE_TOTAL_DISPLAYS + CUBE_DISPLAYS)]
        table_scenes = table_tags.scene_fk.tolist()
        other_scenes = other_tags.scene_fk.tolist()
        total_cube_scenes = total_cube_tags.scene_fk.tolist()
        mixed_scenes = list(set(table_scenes) & set(total_cube_scenes)
                            )  # scenes with total cube tag & table tag
        # scenes = list((set(table_scenes)|set(mixed_scenes))-set(other_scenes))
        scenes = list(set(table_scenes) - set(other_scenes))
        table_bays = pd.DataFrame({})
        table_display = pd.DataFrame({})
        if not table_tags.empty:
            table_display_fk = table_tags['display_fk'].values[0]
            table_display_name = table_tags['display_name'].values[0]
        for scene in scenes:
            cube_size = 0
            table_tags_scene = table_tags[table_tags['scene_fk'] == scene]
            table_bays_scene = table_tags_scene[['scene_fk', 'bay_number']].copy()
            number_of_captured_sides = len(table_tags_scene.groupby(
                ['scene_fk', 'bay_number']).display_size.sum())
            table_size = table_tags_scene['display_size'].values[0]
            if scene in mixed_scenes:
                # add the size of cube (1 cube = 3 tables), all_cube_tags includes cube and total in order to include all products
                all_cube_tags = self.match_display_in_scene[self.match_display_in_scene['display_name'].isin(
                    CUBE_TOTAL_DISPLAYS + CUBE_DISPLAYS)]
                all_cube_tags_scene = all_cube_tags[all_cube_tags['scene_fk'] == scene]
                cube_bays_scene = all_cube_tags_scene[['scene_fk', 'bay_number']].copy()
                cube_size = all_cube_tags_scene.loc[all_cube_tags_scene['display_name'].isin(
                    CUBE_TOTAL_DISPLAYS)].display_size.sum()
            if number_of_captured_sides == 1:
                size = min(table_tags_scene.groupby(['scene_fk', 'bay_number']).display_size.sum())
                display_size = size + (cube_size * 3 * table_size)
            else:
                try:
                    min_side = min(table_tags_scene.groupby(
                        ['scene_fk', 'bay_number']).display_fk.count())
                    max_side = max(table_tags_scene.groupby(
                        ['scene_fk', 'bay_number']).display_fk.count())
                    display_size = (min_side * max_side * table_size) + (cube_size * 3 * table_size)
                except Exception as e:
                    display_size = (cube_size * 3 * table_size)  # table bays are not valid
            table_display = table_display.append({'scene_fk': scene, 'display_fk': table_display_fk,
                                                  'display_size': display_size, 'display_name': table_display_name},
                                                 ignore_index=True)
            table_bays = table_bays.append(table_bays_scene, ignore_index=True)
            if scene in mixed_scenes:
                table_bays = table_bays.append(cube_bays_scene, ignore_index=True)
        if not table_bays.empty:
            table_bays.drop_duplicates(['scene_fk', 'bay_number'], inplace=True)
        if not table_display.empty:
            # only valid tags are relevant
            table_valid_bays = self._filter_valid_bays(table_bays)
            table_display_with_id = self._insert_into_display_surface(table_display)
            table_display_with_id_and_bays = table_display_with_id.merge(
                table_valid_bays, on=['scene_fk'])
            self._calculate_share_of_display(table_display_with_id_and_bays, all_skus=0)

        return

    def get_products_brand(self):
        query = """
                select p.pk as product_fk,b.name as brand_name from static_new.product p
                join static_new.brand b on b.pk = p.brand_fk;
                """
        if not self.project_connector.is_connected:
            self.project_connector = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        res = pd.read_sql_query(query, self.project_connector.db)
        return res

    def get_display_group(self, display_group):
        query = '''select pk
                    from static.custom_entity
                        where name = '{}';'''.format(display_group)

        display_group_fk = pd.read_sql_query(query, self.project_connector.db)
        return display_group_fk.pk[0]

    def _calculate_share_of_display(self, display_with_id_and_bays, all_skus=1):
        """
        Cross information between display and bays to match_product_in_scene.
        Calculating sos of the product on the display in order to calculate its area in the store.
        For sos calculation - stacked tags are not taken into account
        :param display_with_id_and_bays:
        :param all_skus:
        :return:
        """
        if not display_with_id_and_bays.empty:
            # merging between display data and product tags, by scene and bay number
            display_visit = display_with_id_and_bays.merge(
                self.match_product_in_scene, on=['scene_fk', 'bay_number'])
            if display_visit.empty:
                Log.debug(self.log_prefix + ' no product found on the displays')
                return
            display_facings_for_product = display_visit.groupby(['display_surface_fk', 'scene_fk', 'product_fk', 'own',
                                                                 'template_fk', 'display_fk', 'display_size'],
                                                                as_index=False).facings.sum()
            # for sos purposes filtering out stacking tags
            display_visit_stacking = display_visit[display_visit['display_name'].isin(
                TABLE_DISPLAYS)]
            display_visit_stacking.drop(['status', 'stacking_layer'], axis=1)
            display_visit = display_visit[~display_visit['display_name'].isin(TABLE_DISPLAYS)]
            display_visit = display_visit[(display_visit['stacking_layer'] == 1)] \
                .drop(['status', 'stacking_layer'], axis=1)
            display_visit = display_visit.append(display_visit_stacking)
            display_facings_for_product = self._exclude_sos(display_facings_for_product)

            # checking if required to calculate based on all sku's in the display or bottom shelf only (max shelf)
            if not all_skus:
                display_visit = display_visit[display_visit['shelf_number_from_bottom'] == 1]
            if not display_visit.empty:
                # summing facings and length for sos calculation
                display_visit_by_display_product = \
                    display_visit.groupby(['display_surface_fk', 'product_fk', 'template_fk',
                                           'display_size', 'display_fk'], as_index=False)['linear', 'facings'].sum()
                display_visit_by_display_product = self._exclude_sos(
                    display_visit_by_display_product)

                display_visit_by_display = \
                    display_visit_by_display_product[display_visit_by_display_product['in_sos'] == 1] \
                        .groupby(['display_surface_fk'], as_index=False)
                display_tot_linear = display_visit_by_display.linear.sum().rename(columns={
                    'linear': 'tot_linear'})
                display_tot_facings = display_visit_by_display.facings.sum().rename(columns={
                    'facings': 'tot_facings'})
                display_visit_by_display_product_enrich_totals = \
                    display_visit_by_display_product.merge(display_tot_linear, on='display_surface_fk') \
                        .merge(display_tot_facings, on='display_surface_fk')

                display_visit_by_display_product_enrich_sos_type = display_visit_by_display_product_enrich_totals.merge(
                    self.displays, on='display_fk')
                facings_sos_condition = ((display_visit_by_display_product_enrich_sos_type['sos_type_fk'] == 2) &
                                         (display_visit_by_display_product_enrich_sos_type['in_sos'] == 1))
                display_visit_by_display_product_enrich_sos_type.loc[facings_sos_condition, 'product_size'] = \
                    display_visit_by_display_product_enrich_sos_type['facings'] / \
                    display_visit_by_display_product_enrich_sos_type['tot_facings'] * \
                    display_visit_by_display_product_enrich_sos_type['display_size']

                linear_sos_condition = ((display_visit_by_display_product_enrich_sos_type['sos_type_fk'] == 1) &
                                        (display_visit_by_display_product_enrich_sos_type['in_sos'] == 1))
                display_visit_by_display_product_enrich_sos_type.loc[linear_sos_condition, 'product_size'] = \
                    display_visit_by_display_product_enrich_sos_type['linear'] / \
                    display_visit_by_display_product_enrich_sos_type['tot_linear'] * \
                    display_visit_by_display_product_enrich_sos_type['display_size']

                # irrlevant products, should be counted in total facings/ linear of display for product size value,
                #  but should be removed from display size share %
                # sub_category was excluded by customer request

                excluded_products = self.data_provider._data[Fields.SOS_EXCLUDED_PRODUCTS]
                irrelevant_products = self.data_provider.all_products.loc[
                    (self.data_provider.all_products['product_type'] == 'Irrelevant') |
                    (self.data_provider.all_products['sub_category'] == 'Skin Care Men') |
                    (self.data_provider.all_products['product_fk'].isin(excluded_products))

                    ]['product_fk'].tolist()

                not_in_sos_condition = ((display_visit_by_display_product_enrich_sos_type['in_sos'] == 0) |
                                        (display_visit_by_display_product_enrich_sos_type['product_fk'].isin(
                                            irrelevant_products)))
                display_visit_by_display_product_enrich_sos_type.loc[not_in_sos_condition,
                                                                     'product_size'] = 0

                display_visit_summary = \
                    display_visit_by_display_product_enrich_sos_type.drop(['linear', 'tot_linear', 'tot_facings',
                                                                           'display_size', 'sos_type_fk',
                                                                           'facings', 'template_fk',
                                                                           'in_sos', 'display_fk', ], axis=1)

                # This will check which products are a part of brands that have more then 2 facing in the display

                displays = display_visit_summary['display_surface_fk'].unique()
                brands = self.get_products_brand()
                merged_displays = display_facings_for_product.merge(
                    brands, how='left', on='product_fk')
                for current_display in displays:
                    self.valid_facing_product[current_display] = []
                    current_display_products = merged_displays[
                        merged_displays['display_surface_fk'] == current_display]
                    brands_in_display = current_display_products['brand_name'].unique()
                    for brand in brands_in_display:
                        if current_display_products[current_display_products['brand_name'] ==
                                                    brand]['facings'].sum() > 2:
                            self.valid_facing_product[current_display].extend(
                                current_display_products[current_display_products['brand_name'] == brand]['product_fk'])

                display_visit_summary = display_facings_for_product.merge(display_visit_summary, how='left',
                                                                          on=['display_surface_fk', 'product_fk'])
                display_visit_summary['product_size'].fillna(0, inplace=True)
            else:
                display_visit_summary = display_facings_for_product
                display_visit_summary['product_size'] = 0

            display_visit_summary = self.remove_by_facing(display_visit_summary)
            displays = display_visit_summary['display_surface_fk'].unique()
            for display in displays:
                single_display_df = display_visit_summary[display_visit_summary['display_surface_fk'] == display]
                if single_display_df.empty:
                    continue
                total_linear_for_display = single_display_df['product_size'].sum()
                display_size = single_display_df['display_size'].iloc[0]
                if total_linear_for_display != 0:
                    diff_ratio = display_size / float(total_linear_for_display)
                else:
                    diff_ratio = 0
                display_visit_summary.loc[display_visit_summary['display_surface_fk'] == display,
                                          ['product_size']] *= diff_ratio
            display_visit_summary_list_of_dict = display_visit_summary.to_dict('records')
            self._insert_into_display_visit_summary(display_visit_summary_list_of_dict)
            self.insert_into_kpi_scene_results(display_visit_summary_list_of_dict)

    def remove_by_facing(self, df):
        """
        This will return a dataframe of products from the visit where the brand has 2 or more then 2 facings.
        """
        df['to_remove'] = 1
        for i in xrange(len(df)):
            row = df.iloc[i]
            if row['product_fk'] in self.valid_facing_product.get(row['display_surface_fk'], []):
                df.ix[i, 'to_remove'] = 0
        result_df = df[df['to_remove'] == 0]
        result_df = result_df.drop(['to_remove'], axis=1, )
        return result_df

    def _exclude_sos(self, df):
        """
        Calculate 'in_sos' field according to exclude rules.
        :param df:
        :return:
        """
        Log.debug(self.log_prefix + ' calculating in_sos')
        excluded_templates = self.data_provider._data[Fields.SOS_EXCLUDED_TEMPLATES]
        excluded_templates['excluded_templates'] = 1
        excluded_template_products = self.data_provider._data[Fields.SOS_EXCLUDED_TEMPLATE_PRODUCTS]
        excluded_template_products['excluded_template_products'] = 1
        df = df.merge(excluded_templates, how='left', on='template_fk') \
            .merge(excluded_template_products, how='left', on=['product_fk', 'template_fk'])
        condition = (df['excluded_templates'] == 1) | (df['excluded_template_products'] == 1)
        df = df.drop(['excluded_templates', 'excluded_template_products'], axis=1)
        df.loc[condition, 'in_sos'] = 0
        df.loc[~condition, 'in_sos'] = 1

        return df

    def _insert_into_display_visit_summary(self, display_visit_summary_list_of_dict):
        Log.debug(self.log_prefix + ' Inserting to display_item_facts')
        self.cur = self.project_connector.db.cursor()
        query = ''' insert into report.display_item_facts
                        (
                            display_surface_fk
                            , item_id
                            , in_sos
                            , own
                            , product_size
                            ,facings
                        )
                    values
                        {};'''
        if len(display_visit_summary_list_of_dict) == 0:
            Log.info('no displays in this scene')
            return

        for display in display_visit_summary_list_of_dict[:-1]:
            query_line = self._get_query_line(display) + ',' + '{}'
            query = query.format(query_line)
        query = query.format(self._get_query_line(display_visit_summary_list_of_dict[-1]))
        self.cur.execute(query)
        self.project_connector.db.commit()

    @staticmethod
    def _get_query_line(display):
        # consider to use existing code which transforms df into query
        return '(' + \
               str(int(display['display_surface_fk'])) + ',' + \
               str(int(display['product_fk'])) + ',' + \
               str(int(display['in_sos'])) + ',' + \
               str(int(display['own'])) + ',' + \
               str(round(display['product_size'], 2)) + ',' + \
               str(display['facings']) + \
               ')'

    def _get_displays_data(self):
        query = ''' select
                        pk as display_fk
                        ,sos_type_fk
                        ,display_group
                    from
                        static.display;'''
        displays = pd.read_sql_query(query, self.project_connector.db)
        return displays

    @staticmethod
    def _filter_valid_bays(df_with_bays):
        return df_with_bays[(df_with_bays['bay_number'] != -1) & (df_with_bays['bay_number'].notnull())]

    def _delete_previous_data(self):
        """
        Deletes previous data from table: probedata.display_surface (by updating delete_time)
        and report.display_visit_summary. Using temp table for scenes list to prevent a lock
        :return:
        """
        Log.debug(self.log_prefix + ' Deleting existing data in display_surface and display_visit_summary')
        drop_temp_table_query = "drop temporary table if exists probedata.t_scenes_to_delete_displays;"
        queries = [
            drop_temp_table_query,
            """ create temporary table probedata.t_scenes_to_delete_displays as
                select pk as scene_fk from probedata.scene where pk = '{}';""".format(self.scene_id),
            """ delete report.display_item_facts, probedata.display_surface
                from probedata.t_scenes_to_delete_displays
                 join probedata.display_surface
                    on probedata.display_surface.scene_fk = probedata.t_scenes_to_delete_displays.scene_fk
                 left join report.display_item_facts
                    on report.display_item_facts.display_surface_fk = probedata.display_surface.pk;""",
            drop_temp_table_query
        ]
        if not self.project_connector.is_connected:
            self.project_connector = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.cur = self.project_connector.db.cursor()
        for query in queries:
            self.cur.execute(query)

    def _insert_into_display_surface(self, display_surface):
        """
        Inserts into probedata.display_surface the displays identified in each scene and its size.
        For each display it updates the new record pk in order to use as a foreign key when inserting into
        report.display_visit_summary.
        :param display_surface:
        :return:
        """
        # Optional performance improvement
        # 1.Use df instead of to_dict
        Log.debug(self.log_prefix + ' Inserting to probedata.display_surface')
        display_surface_dict = display_surface.to_dict('records')
        query = '''insert into probedata.display_surface
                        (
                            scene_fk
                            , display_fk
                            , surface
                        )
                        values
                         {};'''
        for display in display_surface_dict[:-1]:
            query_line = self._get_display_surface_query_line(display) + ',' + '{}'
            query = query.format(query_line)
        query = query.format(self._get_display_surface_query_line(display_surface_dict[-1]))
        self.cur.execute(query)
        self.project_connector.db.commit()
        last_insert_id = self.cur.lastrowid
        row_count = self.cur.rowcount
        if row_count == len(display_surface_dict):
            for j in range(0, len(display_surface_dict)):
                display_surface_dict[j]['display_surface_fk'] = last_insert_id
                last_insert_id += 1
        else:
            msg = self.log_prefix + ' error: not all display were inserted.'
            Log.error(msg)
            raise Exception(msg)

        return pd.DataFrame(display_surface_dict)

    @staticmethod
    def _get_display_surface_query_line(display):
        return '({0}, {1}, {2})'.format(display['scene_fk'], display['display_fk'], display['display_size'])

    def _get_match_display_in_scene_data(self):
        if not self.project_connector.is_connected:
            self.project_connector = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        local_con = self.project_connector.db
        query = ''' select
                        mds.display_fk
                        ,ds.display_name
                        ,mds.scene_fk
                        ,ds.size as display_size
                        ,mds.bay_number
                    from
                            probedata.match_display_in_scene mds
                        join
                            probedata.match_display_in_scene_info mdici
                             on mdici.pk = mds.match_display_in_scene_info_fk AND mdici.delete_time IS NULL
                        join
                            probedata.stitching_scene_info ssi
                             ON ssi.pk = mdici.stitching_scene_info_fk
                             AND ssi.delete_time IS NULL
                        join
                            static.display ds on ds.pk = mds.display_fk
                        join
                            probedata.scene sc on sc.pk=mds.scene_fk
                             and sc.pk = \'{}\''''.format(self.scene_id)
        match_display_in_scene = pd.read_sql_query(query, local_con)
        return match_display_in_scene

    def _get_match_product_in_scene_data(self):
        query = ''' select
                        mps.scene_fk
                        ,mps.product_fk
                        ,mps.probe_match_fk
                        ,mps.bay_number
                        ,mps.shelf_number
                        ,mps.status
                        ,mps.stacking_layer
                        ,mps.shelf_number_from_bottom
                        ,1 as facings
                        ,(shelf_px_right-shelf_px_left) as 'linear'
                        ,b.manufacturer_fk = 4 as own
                        ,sc.template_fk
                    from
                            probedata.match_product_in_scene mps
                        join
                            static.product p on p.pk = mps.product_fk and mps.status <> 2
                        join
                            static.brand b on b.pk = p.brand_fk
                        join
                            probedata.scene sc on sc.pk = mps.scene_fk
                             and sc.pk = '{0}'
                        join
                            static.template t on t.pk = sc.template_fk
                             and t.is_recognition = 1
                    '''.format(self.scene_id)
        df = pd.read_sql_query(query, self.project_connector.db)
        match_product_in_scene = self.exclude_special_attribute_products(df, 'additional display')
        return match_product_in_scene

    def insert_into_kpi_scene_results(self, display_visit_summary_list_of_dict):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(DISPLAY_SIZE_KPI_NAME)
        df = pd.DataFrame(display_visit_summary_list_of_dict)

        if df.empty:
            return
        df.product_size = df.product_size.round(2)
        final_df = df.groupby(['display_group', 'product_fk'])[
            'product_size', 'facings'].sum().reset_index()
        for index, row in final_df.iterrows():
            if row['product_size'] != 0:
                display_group_fk = self.get_display_group(row['display_group'])
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=display_group_fk,
                                               denominator_id=row['product_fk'],
                                               result=row['product_size'], score=row['facings'], by_scene=True)
        return

    @kpi_runtime()
    def save_nlsos_to_custom_scif(self):
        """
        copied the same calculation as 'gross_len_split_stack' field in scif, used 'width_mm_advance' \
        instead of 'width_mm'.
        :return: save results to both KPI results and pservice.custom_scene_item_facts
        """
        matches = self.matches_from_data_provider.copy()
        if matches.empty or self.scif.empty:
            return
        mask = (matches.status != 2) & (matches.bay_number != -1) & (matches.shelf_number != -1) & \
               (matches.stacking_layer != -1) & (matches.facing_sequence_number != -1)
        matches_reduced = matches[mask]

        # calculate number of products in each stack
        items_in_stack = matches.loc[
            mask, ['scene_fk', 'bay_number', 'shelf_number', 'facing_sequence_number']].groupby(
            ['scene_fk', 'bay_number', 'shelf_number', 'facing_sequence_number']).size().reset_index()
        items_in_stack.rename(columns={0: 'items_in_stack'}, inplace=True)
        matches_reduced = matches_reduced.merge(items_in_stack, how='left',
                                                on=['scene_fk', 'bay_number', 'shelf_number', 'facing_sequence_number'])
        matches_reduced['w_split'] = 1 / matches_reduced.items_in_stack
        matches_reduced['gross_len_split_stack_new'] = matches_reduced['width_mm_advance'] * matches_reduced.w_split
        new_scif_gross_split = matches_reduced[['product_fk', 'scene_fk', 'gross_len_split_stack_new',
                                                'width_mm_advance', 'width_mm']].groupby(
            by=['product_fk', 'scene_fk']).sum().reset_index()

        new_scif = pd.merge(self.scif, new_scif_gross_split,
                            how='left', on=['scene_fk', 'product_fk'])

        # Handle uncorrect gross_len_split_stack_new <= 1 cases.
        new_scif = self.deal_with_empty_advanced_width_mm_values(new_scif).fillna(0)

        self.save_nlsos_as_kpi_results(new_scif)
        self.insert_data_into_custom_scif(new_scif)

    @staticmethod
    def deal_with_empty_advanced_width_mm_values(new_scif):
        relevant_rows = new_scif[(new_scif['gross_len_split_stack'] >= 1) &
                                 ((new_scif['gross_len_split_stack_new'] < 2) |
                                  (new_scif['gross_len_split_stack_new'].isna()))]
        relevant_indices = relevant_rows.reset_index()['index'].tolist()
        for i in relevant_indices:
            new_scif.iloc[i, new_scif.columns.get_loc('gross_len_split_stack_new')] = \
                new_scif.iloc[i]['gross_len_split_stack']
        return new_scif

    @staticmethod
    def calculate_result(num, den):
        if den:
            return num / float(den)
        else:
            return 0

    def save_nlsos_as_kpi_results(self, new_scif):
        """
        Save nlsos results, calculate for each product the nlsos result.
        The calculation includes exluding for relevant and out_of_sos_assortment Products
        :param new_scif: the new scif created with width_mm_advance field
        :return: save the result for each product
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(NEW_LSOS_KPI)
        if kpi_fk is None:
            Log.warning("There is no matching Kpi fk for kpi name: " + NEW_LSOS_KPI)
            return
        new_scif = new_scif[~new_scif['product_fk'].isnull()]
        new_scif_without_irrelevant = new_scif[~(new_scif['product_type'].isin(['Irrelevant']))]
        new_scif_without_excludes = new_scif_without_irrelevant[new_scif_without_irrelevant['rlv_sos_sc'] == 1]
        denominator_result = new_scif_without_excludes.gross_len_split_stack_new.sum()
        for i, row in new_scif_without_excludes.iterrows():
            numerator_result = row['gross_len_split_stack_new']
            result = self.calculate_result(numerator_result, denominator_result)
            result = round(result * 100, 2)
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=row['product_fk'],
                                           denominator_id=self.store_id,
                                           denominator_result=denominator_result,
                                           numerator_result=numerator_result,
                                           result=result, score=result, by_scene=True)

    def insert_data_into_custom_scif(self, new_scif):
        """
        Deletes all previous results (for that scene) and writes the new ones.
        :param new_scif: the df to work on
        :return: saves the data to reportg.custom_scene_item_facts
        """
        session_id = self.data_provider.session_id
        new_scif['session_id'] = session_id
        delete_query = """DELETE FROM pservice.custom_scene_item_facts WHERE session_fk = {} and 
                                                        scene_fk = {}""".format(session_id, self.scene_id)
        insert_query = """INSERT INTO pservice.custom_scene_item_facts \
                            (session_fk, scene_fk, product_fk, in_assortment_osa, length_mm_custom) VALUES """
        for i, row in new_scif.iterrows():
            insert_query += str(tuple(row[['session_id', 'scene_fk',
                                           'product_fk', 'in_assort_sc', 'gross_len_split_stack_new']])) + ", "
        insert_query = insert_query[:-2]
        try:
            self.common.execute_custom_query(delete_query)
        except Exception as ex:
            Log.error("Couldn't delete old results from custom_scene_item_facts, error".format(ex))
            return
        try:
            self.common.execute_custom_query(insert_query)
        except Exception as ex:
            Log.error("Couldn't write new results to custom_scene_item_facts and deleted the old results, "
                      "error {}".format(ex))

    def get_png_manufacturer_fk(self):
        return self.all_products[self.all_products['manufacturer_name'].str.encode("utf8") ==
                                 PNG_MANUFACTURER]['manufacturer_fk'].values[0]

    def _get_display_size_of_product_in_scene(self):
        """
        get product size and item id for DISPLAY_SIZE_PER_SCENE KPI
        :return:
        """
        local_con = self.project_connector.db
        query = '''	select ds.scene_fk as scene_id, dif.item_id, dif.product_size, dif.facings  
                    from 
                            report.display_item_facts dif
                    join
                            probedata.display_surface ds 
                            on ds.pk=dif.display_surface_fk and ds.scene_fk=\'{0}\''''.format(self.scene_id)
        df = pd.read_sql_query(query, local_con)
        return df

    def calculate_display_size(self):
        """
        calculate P&G manufacture percentage
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(DISPLAY_SIZE_PER_SCENE)

        # get size and item id
        df_products_size = self._get_display_size_of_product_in_scene()

        if self.scif.empty or df_products_size.empty:
            return

        filter_scif = self.scif[[u'scene_id', u'item_id',
                                 u'manufacturer_fk', u'rlv_sos_sc', u'status']]
        df_result = pd.merge(filter_scif, df_products_size, on=['item_id', 'scene_id'], how='left')
        df_result = df_result[df_result['product_size'] > 0]

        if kpi_fk:
            denominator = df_result.product_size.sum()  # get all products from scene
            numerator = df_result[df_result['manufacturer_fk'] ==
                                  self.png_manufacturer_fk]['product_size'].sum()  # get P&G products from scene
            if denominator:
                score = numerator / denominator  # get the percentage of P&G products from all products
                numerator, denominator = numerator * 1000, denominator * 1000
            else:
                score = 0
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.png_manufacturer_fk, numerator_result=numerator,
                                           denominator_id=self.store_id, denominator_result=denominator, result=score,
                                           score=score, by_scene=True)
        else:
            print 'the kpi is not configured in db'

    def get_filterd_matches(self):
        """
        remove status=2 , group with scif by 'product_fk' to add 'manufacturer_fk'
        :return:
        """
        # copy the DFs
        a, b = self.matches_from_data_provider.copy(), self.scif.copy()

        if a.empty or b.empty:
            return pd.DataFrame()

        # merge wite scif to add manufacture
        matches_filtered = pd.merge(a, b, how='left',
                                    on=['product_fk', 'scene_fk'])[[u'scene_fk', u'product_fk', 'status_x',
                                                                    'width_mm_x', u'width_mm_advance',
                                                                    u'product_type', u'manufacturer_fk', 'rlv_sos_sc']]
        # rename columns
        matches_filtered.columns = [u'scene_fk', u'product_fk', 'status', 'width_mm', u'width_mm_advance',
                                    u'product_type', u'manufacturer_fk', 'rlv_sos_sc']

        # remove status == 2
        matches_filtered = matches_filtered[matches_filtered['status'] != 2]

        # remove rlv_sos_sc != 1
        matches_filtered = matches_filtered[~matches_filtered['product_fk'].isnull()]
        new_matches_filtered_without_irrelevant = matches_filtered[~(
            matches_filtered['product_type'].isin(['Irrelevant']))]
        new_matches_filtered_without_excludes = new_matches_filtered_without_irrelevant[
            new_matches_filtered_without_irrelevant['rlv_sos_sc'] == 1]

        # sum 'width_mm' and 'width_mm_advance' removing unused columns
        new_matches_filtered_without_excludes = new_matches_filtered_without_excludes[[u'scene_fk', u'manufacturer_fk',
                                                                                       u'product_fk', 'width_mm',
                                                                                       u'width_mm_advance']]

        new_matches_filtered_without_excludes = new_matches_filtered_without_excludes.groupby(['product_fk',
                                                                                               'scene_fk',
                                                                                               'manufacturer_fk'
                                                                                               ]).sum().reset_index()

        return new_matches_filtered_without_excludes

    def calculate_linear_or_presize_linear_length(self, width):
        """
        calculate the manufacturer 'SOS linear length' by 'width_mm' parmeter or
        manufacturer 'SOS presize linear length' by 'width_mm_advance' parmeter.
        :param width: width_mm or width_mm_advance
        :return:
        """

        # choosing the kpi to use depend on the input parameter
        kpi_fk = kpi_name = None
        if width == 'width_mm':
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(LINEAR_SOS_MANUFACTURER_IN_SCENE)
            kpi_name = LINEAR_SOS_MANUFACTURER_IN_SCENE
        elif width == 'width_mm_advance':
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(PRESIZE_LINEAR_SOS_MANUFACTURER_IN_SCENE)
            kpi_name = PRESIZE_LINEAR_SOS_MANUFACTURER_IN_SCENE
        if kpi_fk is None:
            Log.warning("There is no matching Kpi fk for kpi name: " + kpi_name)
            return
        matches_filtered = self.get_filterd_matches()

        if matches_filtered.empty:
            return

        # get the width of P&G products in scene
        numerator = matches_filtered[matches_filtered['manufacturer_fk'] ==
                                     self.png_manufacturer_fk][width].sum()

        # get the width of all products in scene
        denominator = matches_filtered[width].sum()  # get the width of all products in scene

        if denominator:
            # get the percentage of P&G products from all products
            score = numerator / float(denominator)
        else:
            score = 0

        self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.png_manufacturer_fk, numerator_result=numerator,
                                       denominator_id=self.store_id, denominator_result=denominator, result=score,
                                       score=score, by_scene=True)
        return 0

    @kpi_runtime()
    def calculate_linear_length(self):
        """
        calculate P&G manufacture linear length percentage using 'width_mm'
        """
        self.calculate_linear_or_presize_linear_length('width_mm')
        return 0

    @kpi_runtime()
    def calculate_presize_linear_length(self):
        """
        calculate P&G manufacture linear length percentage using 'width_mm_advance'
        """
        self.calculate_linear_or_presize_linear_length('width_mm_advance')
        return 0

    def exclude_special_attribute_products(self, df, smart_attribute):
        """
        Helper to exclude smart_attribute products
        :return: filtered df without smart_attribute products
        """
        if self.match_probe_in_scene.empty:
            return df
        smart_attribute_df = self.match_probe_in_scene[self.match_probe_in_scene['name']
                                                       == smart_attribute]
        if smart_attribute_df.empty:
            return df
        match_product_in_probe_fks = smart_attribute_df['match_product_in_probe_fk'].tolist()
        df = df[~df['probe_match_fk'].isin(match_product_in_probe_fks)]
        return df

    def get_product_special_attribute_data(self, scene_id):
        query = """
                SELECT * FROM probedata.match_product_in_probe_state_value A
                left join probedata.match_product_in_probe B on B.pk = A.match_product_in_probe_fk
                left join static.match_product_in_probe_state C on C.pk = A.match_product_in_probe_state_fk
                left join probedata.probe on probe.pk = probe_fk 
                where C.name = '{}' and scene_fk = {};
            """.format('additional display', scene_id)
        df = pd.read_sql_query(query, self.project_connector.db)
        return df

    def _get_scene_category(self, scene_pk):

        query = """SELECT category.name  as scene_category FROM probedata.scene 
            LEFT JOIN static.template ON template_fk = template.pk 
            LEFT JOIN static_new.category ON product_category_fk = category.pk
            WHERE scene.pk = {};""".format(scene_pk)

        if not self.project_connector.is_connected:
            self.project_connector = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        df = pd.read_sql_query(query, self.project_connector.db)
        return None if df is None else df['scene_category'][0]

