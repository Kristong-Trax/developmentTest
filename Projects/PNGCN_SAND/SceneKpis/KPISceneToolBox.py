#!/usr/bin/env python
# -*- coding: utf-8 -*

import os
import numpy as np
import pandas as pd

from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.PNGCN_SAND.ShareOfDisplay.ExcludeDataProvider import ShareOfDisplayDataProvider, Fields
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
import KPIUtils_v2.Utils.Parsers.ParseInputKPI as Parser
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

__Author__ = 'Dudi_s'

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
PRESIZE_LINEAR_LENGTH_PER_LENGTH = 'PRESIZE_LINEAR_LENGTH_PER_LENGTH'

# Eye level KPI
EYE_LEVEL_KPI = "Eye_level_kpi_PER_SCENE"
OLAY_BRAND = 'Olay'
SAFEGUARD_BRAND = 'Safeguard'
PCC_CATEGORY = 'Personal Cleaning Care'
HANDWASH_SUB_CATEGORY = 'Handwash'
BODYWASH_SUB_CATEGORY = 'Bodywash'
OTHER_SUB_CATEGORY = 'Other'
PCC_BAR_SUB_CATEGORY = 'PCC-Bar'
PCC_FILTERS = {
'SFG Bodywash':  {'population':
                {'include': [{"manufacturer_name": [PNG_MANUFACTURER],"category": [PCC_CATEGORY],
                              "brand_name": [SAFEGUARD_BRAND], 'sub_category': [BODYWASH_SUB_CATEGORY]}],
                 'exclude': {},
                 'include_operator': 'and'}},
'SFG Handwash':  {'population':
                {'include': [{"manufacturer_name": [PNG_MANUFACTURER],"category": [PCC_CATEGORY],
                              "brand_name": [SAFEGUARD_BRAND], 'sub_category': [HANDWASH_SUB_CATEGORY]}],
                 'exclude': {},
                 'include_operator': 'and'}},
'SFG Other':     {'population':
                {'include': [{"manufacturer_name": [PNG_MANUFACTURER],"category": [PCC_CATEGORY],
                              "brand_name": [SAFEGUARD_BRAND], 'sub_category': [OTHER_SUB_CATEGORY]}],
                 'exclude': {},
                 'include_operator': 'and'}},
'SFG PCCBAR':    {'population':
                {'include': [{"manufacturer_name": [PNG_MANUFACTURER],"category": [PCC_CATEGORY],
                              "brand_name": [SAFEGUARD_BRAND], 'sub_category': [PCC_BAR_SUB_CATEGORY]}],
                 'exclude': {},
                 'include_operator': 'and'}},
'OLAY Bodywash': {'population':
                {'include': [{"manufacturer_name": [PNG_MANUFACTURER],"category": [PCC_CATEGORY],
                              "brand_name": [OLAY_BRAND], 'sub_category': [BODYWASH_SUB_CATEGORY]}],
                 'exclude': {},
                 'include_operator': 'and'}},
'OLAY Handwash':  {'population':
                {'include': [{"manufacturer_name": [PNG_MANUFACTURER],"category": [PCC_CATEGORY],
                              "brand_name": [OLAY_BRAND], 'sub_category': [HANDWASH_SUB_CATEGORY]}],
                 'exclude': {},
                 'include_operator': 'and'}},
'OLAY Other':     {'population':
                {'include': [{"manufacturer_name": [PNG_MANUFACTURER],"category": [PCC_CATEGORY],
                              "brand_name": [OLAY_BRAND], 'sub_category': [OTHER_SUB_CATEGORY]}],
                 'exclude': {},
                 'include_operator': 'and'}},
'OLAY PCCBAR':    {'population':
                {'include': [{"manufacturer_name": [PNG_MANUFACTURER],"category": [PCC_CATEGORY],
                              "brand_name": [OLAY_BRAND], 'sub_category': [PCC_BAR_SUB_CATEGORY]}],
                 'exclude': {},
                 'include_operator': 'and'}},
'Compatitor PCC': {'population': {'include': [{"category": [PCC_CATEGORY]}],
                 'exclude': {"manufacturer_name": [PNG_MANUFACTURER]},
                 'include_operator': 'and'}},
'PNGOTHER':       {'population':
                {'include': [{"manufacturer_name": [PNG_MANUFACTURER]}],
                 'exclude': {"category": [PCC_CATEGORY]},
                 'include_operator': 'and'}},
'Competitor Other':  {'population':
                {'include': [{}],
                 'exclude': {"manufacturer_name": [PNG_MANUFACTURER], "category": [PCC_CATEGORY]},
                 'include_operator': 'and'}}
}

class PngcnSceneKpis(object):
    def __init__(self, project_connector, common, scene_id, data_provider=None):
        # self.session_uid = session_uid
        self.scene_id = scene_id
        self.project_connector = project_connector
        if data_provider is not None:
            self.data_provider = data_provider
            # self.project_connector = data_provider.project_connector
            self.on_ace = True
        else:
            self.on_ace = False
            # self.data_provider = PNGCN_SANDShareOfDisplayDataProvider(project_connector, self.session_uid)

        self.tools = GENERALToolBox(self.data_provider)
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
        self.store_id = self.data_provider[Data.SESSION_INFO].store_fk.values[0]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.png_manufacturer_fk = self.get_png_manufacturer_fk()
        self.psdataprovider = PsDataProvider(data_provider=self.data_provider)
        self.parser = Parser

    def process_scene(self):
        try:
            self.save_nlsos_to_custom_scif()
        except Exception as e:
            Log.error('nlsos to custom scif failed for scene: \'{0}\' error: {1}'.format(self.scene_id, str(e)))
            raise e
        try:
            self.calculate_eye_level_kpi()
        except Exception as e:
            Log.error('eye-level kpi failed for scene: \'{0}\' error: {1}'.format(self.scene_id, str(e)))
            raise e
        try:
            Log.debug(self.log_prefix + ' Retrieving data')
            self.calculate_display_size()
            self.calculate_linear_length()
            self.calculate_presize_linear_length()
            self.match_display_in_scene = self._get_match_display_in_scene_data()
            # if there are no display tags there's no need to retrieve the rest of the data.
            if self.match_display_in_scene.empty:
                Log.debug(self.log_prefix + ' No display tags')
                self._delete_previous_data()
                self.common.commit_results_data(result_entity='scene')
            else:
                self.displays = self._get_displays_data()
                self.match_product_in_scene = self._get_match_product_in_scene_data()
                self._delete_previous_data()
                self._handle_promotion_wall_display()
                self._handle_cube_or_4_sided_display()
                self._handle_table_display()
                self._handle_rest_display()
                self.common.commit_results_data(result_entity='scene')
                if self.on_ace:
                    Log.debug(self.log_prefix + ' Committing share of display calculations')
                    self.project_connector.db.commit()
                Log.info(self.log_prefix + ' Finished calculation')
        except Exception as e:
            Log.error('Share of display calculation for scene: \'{0}\' error: {1}'.format(
                self.scene_id, str(e)))
            raise e


    def calculate_eye_level_kpi(self):
        if self.matches_from_data_provider.empty:
            return
        relevant_templates = self.psdataprovider.get_scene_category_data(PCC_CATEGORY)['template_fk'].tolist()
        try:
            template_fk = self.data_provider.scenes_info['template_fk'].values[0]
            if template_fk not in relevant_templates:
                return
        except:
            Log.error("Couldn't find scene type for scene number {}".format(str(self.scene_id)))
            return
        entity_df = self.psdataprovider.get_custom_entities_df('eye_level_fragments')
        if entity_df.empty:
            return
        df = self.get_eye_level_shelves(self.matches_from_data_provider)
        full_df = pd.merge(df,self.all_products,on="product_fk")
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(EYE_LEVEL_KPI)
        results_df = pd.DataFrame(columns=['fk', 'numerator_id', 'result', 'numerator_result',
                                             'denominator_result', 'denominator_id', 'score', 'context_id', 'by_scene'])
        for key in PCC_FILTERS.keys():
            frag_df = self.parser.filter_df(PCC_FILTERS[key], full_df)
            full_df.drop(frag_df.index, axis=0, inplace=True)
            frag_df.sort_values(by=['bay_number', 'shelf_number', 'facing_sequence_number'], inplace=True)
            seq_df = frag_df.copy()
            seq_df['group'] = ((frag_df.product_fk != frag_df.product_fk.shift())
                                | (frag_df.shelf_number != frag_df.shelf_number.shift())
                                | (frag_df.bay_number != frag_df.bay_number.shift())).cumsum()
            frag_df = seq_df.groupby(by=['group']).first()
            frag_df['facings'] = seq_df.groupby(by=['group']).size()
            # frag_df['is_new_sequence'] = ((frag_df.shelf_number != frag_df.shelf_number.shift()) |
            #                               (frag_df.bay_number != frag_df.bay_number.shift()))
            # facing_sequence_number = 0
            for i, row in frag_df.iterrows():
                # if row['is_new_sequence']:
                #     facing_sequence_number = 0
                # facing_sequence_number += 1
                facing_sequence_number = row['facing_sequence_number']
                entity_fk = entity_df[entity_df['entity_name'] == key]['entity_fk'].values[0]
                product_fk = row['product_fk']
                bay_number = row['bay_number']
                shelf_number = row['shelf_number']
                category_fk = row['category_fk']
                facings = row['facings']

                results_df = results_df.append({'fk': kpi_fk, 'numerator_id':product_fk,
                                'result':facing_sequence_number, 'numerator_result':bay_number,
                                'denominator_result':shelf_number, 'denominator_id':entity_fk, 'score':facings,
                                'context_id':category_fk, 'by_scene':True}, ignore_index=True)

        results_df.sort_values(by=['numerator_result', 'denominator_result', 'result'], inplace=True)
        for i, row in results_df.iterrows():
            self.common.write_to_db_result(**row)
        # self.common.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=facing_sequence_number,
        #                                numerator_result=bay_number, denominator_result=shelf_number,
        #                                denominator_id=entity_fk, score=facings, context_id=category_fk,
        #                                by_scene=True)

    def get_eye_level_shelves(self, df):
        if df.empty:
            return df
        df = df[df['stacking_layer'] == 1]
        bay_and_shelves = df.groupby(by=['bay_number', 'shelf_number']).first().reset_index()[['bay_number', 'shelf_number']]
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
            bay_df = df[(df['bay_number'] == bay_number) & (df['shelf_number'].isin(shelves_to_choose))]
            bays_df.append(bay_df)
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
        scenes = list(set(scenes)-set(mixed_with_table_scenes))
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
        scenes = list(set(table_scenes)-set(other_scenes))
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
                                                  'display_size': display_size, 'display_name': table_display_name}, ignore_index=True)
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
        self.project_connector.disconnect_rds()
        self.project_connector.connect_rds()
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
            display_visit = display_visit[(display_visit['stacking_layer'] == 1)]\
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
                    display_visit_by_display_product \
                    .merge(display_tot_linear, on='display_surface_fk') \
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
                irrelvant_products = self.data_provider.all_products.loc[
                    (self.data_provider.all_products['product_type'] == 'Irrelevant') |
                    (self.data_provider.all_products['sub_category'] == 'Skin Care Men')|
                    (self.data_provider.all_products['product_fk'].isin(excluded_products))

                ]['product_fk'].tolist()

                not_in_sos_condition = ((display_visit_by_display_product_enrich_sos_type['in_sos'] == 0) |
                                        (display_visit_by_display_product_enrich_sos_type['product_fk'].isin(irrelvant_products)))
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
                        if current_display_products[current_display_products['brand_name'] == brand]['facings'].sum() > 2:
                            self.valid_facing_product[current_display].extend(
                                current_display_products[current_display_products['brand_name'] == brand]['product_fk'])

                display_visit_summary = display_facings_for_product.merge(display_visit_summary, how='left',
                                                                          on=['display_surface_fk', 'product_fk'])
                display_visit_summary['product_size'].fillna(0, inplace=True)
            else:
                display_visit_summary = display_facings_for_product
                display_visit_summary['product_size'] = 0

            display_visit_summary = self.remove_by_facing(display_visit_summary)
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

        # excluded_products = self.data_provider._data[Fields.SOS_EXCLUDED_PRODUCTS]
        # excluded_products['excluded_products'] = 1

        # df = df.merge(excluded_templates, how='left', on='template_fk') \
        #        .merge(excluded_products, how='left', on='product_fk') \
        #        .merge(excluded_template_products, how='left', on=['product_fk', 'template_fk'])

        df = df.merge(excluded_templates, how='left', on='template_fk') \
               .merge(excluded_template_products, how='left', on=['product_fk', 'template_fk'])

        # condition = (df['excluded_templates'] == 1) | \
        #             (df['excluded_template_products'] == 1) | (df['excluded_products'] == 1)

        condition = (df['excluded_templates'] == 1) | (df['excluded_template_products'] == 1)

        # df = df.drop(['excluded_templates', 'excluded_template_products',
        #               'excluded_products'], axis=1)
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

    def _get_session_info(self):
        query = ''' select pk as session_fk, store_fk, visit_date, session_uid
                    from probedata.session where session_uid = \'{}\''''.format(self.session_uid)
        session_info = pd.read_sql_query(query, self.project_connector.db)
        return session_info

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
        self.project_connector.disconnect_rds()
        self.project_connector.connect_rds()
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
        match_product_in_scene = pd.read_sql_query(query, self.project_connector.db)
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
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=display_group_fk, denominator_id=row['product_fk'],
                                               result=row['product_size'], score=row['facings'], by_scene=True)
        return

    def save_nlsos_to_custom_scif(self):
        matches = self.matches_from_data_provider.copy()
        if matches.empty or self.scif.empty:
            return
        mask = (matches.status != 2) & (matches.bay_number != -1) & (matches.shelf_number != -1) & \
               (matches.stacking_layer != -1) & (matches.facing_sequence_number != -1)
        matches_reduced = matches[mask]

        # calculate number of products in each stack
        items_in_stack = matches.loc[mask, ['scene_fk','bay_number', 'shelf_number', 'facing_sequence_number']].groupby(
            ['scene_fk','bay_number', 'shelf_number', 'facing_sequence_number']).size().reset_index()
        items_in_stack.rename(columns={0: 'items_in_stack'}, inplace=True)
        matches_reduced = matches_reduced.merge(items_in_stack, how='left',
                                                on=['scene_fk','bay_number', 'shelf_number', 'facing_sequence_number'])
        matches_reduced['w_split'] = 1 / matches_reduced.items_in_stack
        matches_reduced['gross_len_split_stack_new'] = matches_reduced['width_mm_advance'] * matches_reduced.w_split
        new_scif_gross_split = matches_reduced[['product_fk','scene_fk','gross_len_split_stack_new',
                                'width_mm_advance']].groupby(by=['product_fk','scene_fk']).sum().reset_index()
        new_scif = pd.merge(self.scif, new_scif_gross_split, how='left',on=['scene_fk','product_fk'])
        new_scif = new_scif.fillna(0)
        self.save_nlsos_as_kpi_results(new_scif)
        self.insert_data_into_custom_scif(new_scif)

    def save_nlsos_as_kpi_results(self, new_scif):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(NEW_LSOS_KPI)
        if kpi_fk is None:
            Log.warning("There is no matching Kpi fk for kpi name: " + NEW_LSOS_KPI)
            return
        new_scif = new_scif[~new_scif['product_fk'].isnull()]
        for i, row in new_scif.iterrows():
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=row['product_fk'], denominator_id=self.store_id,
                                           result=row['gross_len_split_stack_new'],
                                           score=row['gross_len_split_stack_new'], by_scene=True)

    def insert_data_into_custom_scif(self, new_scif):
        session_id = self.data_provider.session_id
        new_scif['session_id'] = session_id
        delete_query = """DELETE FROM pservice.custom_scene_item_facts WHERE session_fk = {} and 
                                                        scene_fk = {}""".format(session_id, self.scene_id)
        insert_query = """INSERT INTO pservice.custom_scene_item_facts \
                            (session_fk, scene_fk, product_fk, in_assortment_osa, length_mm_custom) VALUES """
        for i, row in new_scif.iterrows():
            insert_query += str(tuple(row[['session_id', 'scene_fk',
                                           'product_fk', 'in_assort_sc','gross_len_split_stack_new']])) + ", "
        insert_query = insert_query[:-2]
        try:
            self.common.execute_custom_query(delete_query)
        except:
            Log.error("Couldn't delete old results from custom_scene_item_facts")
            return
        try:
            self.common.execute_custom_query(insert_query)
        except:
            Log.error("Couldn't write new results to custom_scene_item_facts and deleted the old results")

    def get_png_manufacturer_fk(self):
        return self.all_products[self.all_products['manufacturer_name'].str.encode("utf8") ==
                                 PNG_MANUFACTURER]['manufacturer_fk'].values[0]

    def calculate_display_size(self):
        """
        calculate P&G manufacture percentage
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(DISPLAY_SIZE_PER_SCENE)
        if kpi_fk:
            denominator = self.scif.facings.sum()  # get all products from scene
            numerator = self.scif[self.scif['manufacturer_fk'] ==
                                  self.png_manufacturer_fk]['facings'].sum()  # get P&G products from scene
            if denominator:
                score = numerator / denominator  # get the percentage of P&G products from all products
            else:
                score = 0
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.png_manufacturer_fk, numerator_result=numerator,
                                           denominator_id=self.store_id, denominator_result=denominator, result=score,
                                           score=score, by_scene=True)
        else:
            print 'the kpi is not configured in db'

    def calculate_linear_length(self):
        """
        calculate P&G manufacture linear length percentage
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(LINEAR_SOS_MANUFACTURER_IN_SCENE)
        filters = {'manufacturer_fk': [self.png_manufacturer_fk]}
        sos = SOS(self.data_provider)
        score, numerator, denominator = \
            sos.calculate_linear_share_of_shelf_with_numerator_denominator(filters, {})
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.png_manufacturer_fk, numerator_result=numerator,
                                       denominator_id=self.store_id, denominator_result=denominator, result=score,
                                       score=score, by_scene=True)
        return 0

    def calculate_presize_linear_length(self):
        """
        used instead of calculating P&G manufacture products presize out off all products in scene
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(PRESIZE_LINEAR_LENGTH_PER_LENGTH)
        numerator = self.scif.width_mm.sum()  # get the width of P&G products in scene
        denominator = self.matches_from_data_provider.width_mm.sum() # get the width of all products in scene
        if denominator:
            score = numerator / denominator  # get the percentage of P&G products from all products
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.png_manufacturer_fk, numerator_result=numerator,
                                           denominator_id=self.store_id, denominator_result=denominator, result=score,
                                           score=score, by_scene=True)
        return 0

#
# if __name__ == '__main__':
#     # Config.init()
#     LoggerInitializer.init('TREX')
#     conn = PSProjectConnector('integ3', DbUsers.CalculationEng)
#     # session = 'd4b46e1d-b4ed-406d-a20b-8ec9b64f5c5e'
#     # session = '14f71194-e843-4a3c-94f0-f712775e8ea2'
#     # session = '9f0244de-66c3-4378-8b46-16c9f79dc978'
#     # # session = 'a6785107-4a59-4aec-943e-8f710c2aa46d'
#     # session = 'ff0a6857-52c8-4bbf-8f4b-697b141fcb9a'
#     session = '26a1aea8-39a0-4d6c-af49-10a20ba4a885'
#     data_provider = ACEDataProvider('integ3')
#     data_provider.load_session_data(session)
#     calculate(conn, session, data_provider)
