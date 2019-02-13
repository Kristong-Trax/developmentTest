#!/usr/bin/env python
# -*- coding: utf-8 -*

import pandas as pd
import os

from datetime import datetime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils.Calculations.Survey import Survey
from Projects.INBEVBR.Utils.GeneralToolBox import INBEVBRGENERALToolBox
from Projects.INBEVBR.Data.Const import Const
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.CommonV2 import Common

__author__ = 'ilays'

KPI_NEW_TABLE = 'report.kpi_level_2_results'
PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Ambev template v3.7 - KENGINE - JANUARY2018.xlsx')

def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result
        return wrapper
    return decorator


class INBEVBRToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = INBEVBRGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_fk = self.store_info['store_fk'].values[0]

        try:
            self.store_type_filter = self.store_info['store_type'].values[0].strip()
        except:
            Log.error("there is no store type in the db")
            return
        try:
            self.region_name_filter = self.store_info['region_name'].values[0].strip()
        except:
            Log.error("there is no region in the db")
            return
        try:
            self.state_name_filter = self.store_info['additional_attribute_2'].values[0].strip()
        except:
            Log.error("there is no state in the db")
            return
        self.kpi_results_queries = []
        self.survey = Survey(self.data_provider, self.output)
        self.kpi_results_new_tables_queries = []
        self.session_id = self.data_provider.session_id
        self.prices_per_session = PsDataProvider(self.data_provider, self.output).get_price(self.session_id)
        self.common_db = Common(self.data_provider)
        self.sos_sheet = pd.read_excel(PATH, Const.SOS).fillna("")
        self.sos_packs_sheet = pd.read_excel(PATH, Const.SOS_PACKS).fillna("")
        self.count_sheet = pd.read_excel(PATH, Const.COUNT).fillna("")
        self.group_count_sheet = pd.read_excel(PATH, Const.GROUP_COUNT).fillna("")
        self.survey_sheet = pd.read_excel(PATH, Const.SURVEY).fillna("")
        self.prod_seq_sheet = pd.read_excel(PATH, Const.PROD_SEQ).fillna("")
        self.prod_seq_2_sheet = pd.read_excel(PATH, Const.PROD_SEQ_2).fillna("")
        self.prod_weight_sku_sheet = pd.read_excel(PATH, Const.PROD_WEIGHT_SKU).fillna("")
        self.prod_weight_subbrand_sheet = pd.read_excel(PATH, Const.PROD_WEIGHT_SUBBRAND).fillna("")
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.parent_kpis = dict.fromkeys([Const.CERVEJA,Const.GAME_PLAN, Const.NAB], 0)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpis_sheet = pd.read_excel(PATH, Const.KPIS).fillna("")
        for index, row in kpis_sheet.iterrows():
            self.handle_atomic(row)
        self.write_parent_kpis_results()
        self.common_db.commit_results_data()

    def handle_atomic(self, row):
        atomic_id = row[Const.KPI_ID]
        atomic_name = row[Const.ENGLISH_KPI_NAME].strip()
        parent_kpi = row[Const.PARENT_KPI_GROUP].strip()
        store_type_template = row[Const.STORE_TYPE_TEMPLATE].strip()

        # if cell in template is not empty
        if store_type_template != "":
            store_types = store_type_template.split(",")
            store_types = [item.strip() for item in store_types]
            if self.store_type_filter not in store_types:
                return

        kpi_type = row[Const.KPI_TYPE].strip()
        if kpi_type == Const.SOS:
            self.handle_sos_atomics(atomic_id, atomic_name, parent_kpi)
        elif kpi_type == Const.SOS_PACKS:
            self.handle_sos_packs_atomics(atomic_id, atomic_name, parent_kpi)
        elif kpi_type == Const.COUNT:
            self.handle_count_atomics(atomic_id, atomic_name, parent_kpi)
        elif kpi_type == Const.GROUP_COUNT:
            self.handle_group_count_atomics(atomic_id, atomic_name, parent_kpi)
        elif kpi_type == Const.SURVEY:
            self.handle_survey_atomics(atomic_id, atomic_name)
        elif kpi_type == Const.PROD_SEQ:
            self.handle_prod_seq_atomics(atomic_id, atomic_name, parent_kpi)
        elif kpi_type == Const.PROD_SEQ_2:
            self.handle_prod_seq_2_atomics(atomic_id, atomic_name, parent_kpi)
        elif kpi_type == Const.PROD_WEIGHT:
            self.handle_prod_weight_atomics(atomic_id, atomic_name, parent_kpi)

    def handle_sos_atomics(self,atomic_id, atomic_name, parent_kpi):

        denominator_number_of_total_facings = 0
        count_result = 0

        # bring the kpi rows from the sos sheet
        rows = self.sos_sheet.loc[self.sos_sheet[Const.KPI_ID] == atomic_id]

        # get a single row
        row = self.find_row(rows)
        if row.empty:
            return

        target = row[Const.TARGET].values[0]
        weight = row[Const.WEIGHT].values[0]
        df = self.scif.copy()

        product_size = row[Const.PRODUCT_SIZE].values[0]
        if product_size != "":
            df = self.filter_product_size(df, product_size)

        # get the filters
        filters = self.get_filters_from_row(row.squeeze())
        numerator_number_of_facings = self.count_of_facings(df, filters)
        if numerator_number_of_facings != 0:
            if 'manufacturer_name' in filters.keys():
                for f in ['manufacturer_name', 'brand_name']:
                    if f in filters:
                        del filters[f]
                denominator_number_of_total_facings = self.count_of_facings(df, filters)
                percentage = 100 * (numerator_number_of_facings / denominator_number_of_total_facings)
                count_result = weight if percentage >= target else 0

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_type(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.parent_kpis[parent_kpi] += count_result

        self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, identifier_parent=parent_kpi,
                                   numerator_result=numerator_number_of_facings, denominator_id = self.store_fk,
                                   denominator_result_after_actions = 3,should_enter=True,
                                   denominator_result=denominator_number_of_total_facings, result=count_result)

    def handle_sos_packs_atomics(self,atomic_id, atomic_name, parent_kpi):

        count_result = 0

        # bring the kpi rows from the sos sheet
        rows = self.sos_packs_sheet.loc[self.sos_packs_sheet[Const.KPI_ID] == atomic_id]

        # get a single row
        row = self.find_row(rows)
        if row.empty:
            return

        # enter only if there is a matching store, region and state
        target = row[Const.TARGET].values[0]
        target_secondary = row[Const.SECONDARY_TARGET].values[0]
        target_packs = row[Const.PACKS_TARGET].values[0]
        result_type = row[Const.RESULT_TYPE].values[0]
        weight = row[Const.WEIGHT].values[0]

        # get the filters
        filters = self.get_filters_from_row(row.squeeze())

        df = self.match_product_in_scene.copy()
        df = pd.merge(df, self.all_products, on="product_fk")
        product_size = row[Const.PRODUCT_SIZE].values[0]
        if product_size != "":
            df = self.filter_product_size(df, product_size)

        df_packs = self.count_of_scenes_packs(df, filters)
        df_packs = df_packs[df_packs['num_packs'] >= target_packs]
        number_of_valid_scenes = len(df_packs)

        if (number_of_valid_scenes >= target_secondary):
            count_result = weight

        # count number of facings
        if ('form_factor' in filters.keys()):
            del filters['form_factor']
        df_numirator = self.count_of_scenes_facings(df,filters)
        # count_of_total_facings = self.count_of_facings(df,filters)
        df_numirator = df_numirator.rename(columns={'face_count': 'facings_nom'})
        count_of_total_facings = df_numirator['facings_nom'].sum()
        for f in ['manufacturer_name', 'brand_name']:
            if f in filters:
                del filters[f]
        df_denominator = self.count_of_scenes_facings(df, filters)
        scene_types_groupby = pd.merge(df_numirator, df_denominator, how='left', on='scene_fk')
        df_target_filtered = scene_types_groupby[(scene_types_groupby['facings_nom'] /
                                                                scene_types_groupby['face_count']) * 100 >= target]
        number_of_valid_scenes = len(df_target_filtered)
        if target_secondary == "":
            target_secondary = 1
        if number_of_valid_scenes >= target_secondary:
            count_result = weight

        number_of_valid_scenes = len(set(df_target_filtered['scene_fk']).union(set(df_packs['scene_fk'])))
        number_of_not_valid_scenes = len(df_denominator['scene_fk'].drop_duplicates())

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_type(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.parent_kpis[parent_kpi] += count_result

        if result_type == 1:
            self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, denominator_id=self.store_fk,
                                denominator_result_after_actions=3,
                                numerator_result=number_of_valid_scenes, identifier_parent=parent_kpi,
                                denominator_result=number_of_not_valid_scenes, result=count_result, should_enter=True)
        elif result_type == 2:
            self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, denominator_id=self.store_fk,
                                numerator_result=count_of_total_facings, identifier_parent=parent_kpi,
                                denominator_result_after_actions=2,
                                denominator_result=0, result=count_result, should_enter=True)
        elif result_type == 3:
            self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, denominator_id=self.store_fk,
                                numerator_result=number_of_valid_scenes, identifier_parent=parent_kpi,
                                denominator_result_after_actions=2,
                                denominator_result=0, result=count_result, should_enter=True)

    def handle_count_atomics(self, atomic_id, atomic_name, parent_kpi):

        # bring the kpi rows from the count sheet
        rows = self.count_sheet.loc[self.count_sheet[Const.KPI_ID] == atomic_id]

        # get a single row
        row = self.find_row(rows)
        if row.empty:
            return

        target = row[Const.TARGET].values[0]
        weight = row[Const.WEIGHT].values[0]
        result_type = row[Const.RESULT_TYPE].values[0]
        count_type = row[Const.COUNT_TYPE].values[0].strip()

        df = self.scif.copy()
        product_size = row[Const.PRODUCT_SIZE].values[0]
        if product_size != "":
            df = self.filter_product_size(df, product_size)

        # get the filters
        filters = self.get_filters_from_row(row.squeeze())

        # if count_type == Const.FACING:
        number_of_facings = self.count_of_facings(df, filters)
        count_result = weight if number_of_facings >= target else 0
        if count_type == Const.SCENES:
            secondary_target = row[Const.SECONDARY_TARGET].values[0]
            scene_types_groupby = self.count_of_scenes(df, filters)
            number_of_scenes = len(scene_types_groupby[scene_types_groupby['facings'] >= target])
            count_result = weight if (number_of_scenes >= secondary_target) else 0

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_type(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.parent_kpis[parent_kpi] += count_result

        if result_type == 1:
            self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1,
                                            numerator_result=0, denominator_id=self.store_fk, identifier_parent=parent_kpi,
                                            denominator_result_after_actions=1,
                                            denominator_result=0, result=count_result, should_enter=True)
        elif result_type == 2:
            self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, identifier_parent=parent_kpi,
                                            numerator_result=number_of_facings, denominator_id=self.store_fk,
                                            denominator_result_after_actions=2,
                                            denominator_result=0, result=count_result, should_enter=True)

    def handle_group_count_atomics(self, atomic_id, atomic_name, parent_kpi):

        count_result = 0
        total_facings_number = 0
        group_score = 0
        result_type = 0
        group_scenes = set()

        # bring the kpi rows from the group_count sheet
        rows = self.group_count_sheet.loc[self.group_count_sheet[Const.KPI_ID] == atomic_id]

        # get a single row
        matching_rows = self.find_row(rows)

        for index, row in matching_rows.iterrows():
            target = row[Const.TARGET]
            weight = row[Const.WEIGHT]
            score = row[Const.SCORE]
            result_type = row[Const.RESULT_TYPE]

            # get the filters
            filters = self.get_filters_from_row(row)

            number_of_facings = self.count_of_facings(self.scif, filters)
            total_facings_number += number_of_facings
            if number_of_facings >= target:
                count_of_scenes = self.count_of_scenes(self.scif, filters)
                group_scenes = group_scenes.union(set(count_of_scenes['scene_id']))
                group_score += score
                if group_score >= 100:
                    count_result = weight

        number_of_valid_scenes = len(group_scenes)
        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_type(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.parent_kpis[parent_kpi] += count_result

        if result_type == 1:
            self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, denominator_id=self.store_fk,
                                            numerator_result=total_facings_number, identifier_parent=parent_kpi,
                                            denominator_result_after_actions=2,
                                            denominator_result=0, result=count_result, should_enter=True)
        elif count_result == 2:
            self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, denominator_id=self.store_fk,
                                            numerator_result=number_of_valid_scenes, identifier_parent=parent_kpi,
                                            denominator_result_after_actions=2,
                                            denominator_result=0, result=count_result, should_enter=True)



    def find_row(self, rows):
        temp = rows[Const.STORE_TYPE_TEMPLATE]
        rows_stores_filter = rows[(temp == self.store_type_filter) | (temp == "")]
        temp = rows_stores_filter[Const.REGION_TEMPLATE]
        rows_regions_filter = rows_stores_filter[(temp == self.region_name_filter) | (temp == "")]
        temp = rows_regions_filter[Const.STATE_TEMPLATE]
        row_result = rows_regions_filter[(temp.apply(lambda r: self.state_name_filter in r.split(",")))
                                                                                               | (temp == "")]
        return row_result


    def filter_product_size(self, df, product_size):

        ml_df = df[df['size_unit'] == 'ml']
        l_df = df[df['size_unit'] == 'l']

        temp_df = l_df.copy()
        temp_df['size'] = l_df['size'].apply((lambda x: x * 1000))
        filtered_df = pd.concat([temp_df,ml_df])
        product_size = str(product_size)
        product_size = product_size.split(',')
        filtered_df = filtered_df[filtered_df['size'].isin(product_size)]
        return filtered_df



    def get_filters_from_row(self, row):
        filters = dict(row)

        # no need to be accounted for
        for field in Const.DELETE_FIELDS:
            if field in filters:
                del filters[field]

        # filter all the empty cells
        for key in filters.keys():
            if (filters[key] == ""):
                del filters[key]
            elif isinstance(filters[key], tuple):
                filters[key] = (filters[key][0].split(","), filters[key][1])
            else:
                filters[key] = filters[key].split(",")
                filters[key] = [item.strip() for item in filters[key]]

        return self.create_filters_according_to_scif(filters)

    def create_filters_according_to_scif(self, filters):
        convert_from_scif =    {Const.TEMPLATE_GROUP: 'template_group',
                                Const.BRAND: 'brand_name',
                                Const.SUB_BRAND: 'sub_brand',
                                Const.CATEGORY: 'category',
                                Const.SUB_CATEGORY: 'sub_category',
                                Const.TEMPLATE_NAME: 'template_name',
                                Const.MANUFACTURER: 'manufacturer_name',
                                Const.CONTAINER_TYPE: 'form_factor',
                                Const.PRODUCT: 'product_name',
                                Const.ATT1: 'att1',
                                Const.FLAVOR: 'Flavor',
                                Const.BEER_TYPE: 'att2'}

        for key in filters.keys():
            filters[convert_from_scif[key]] = filters.pop(key)
        return filters

    def count_of_facings(self, df, filters):

        facing_data = df[self.tools.get_filter_condition(df, **filters)]
        number_of_facings = facing_data['facings'].sum()
        return number_of_facings

    def count_of_scenes_packs(self, df, filters):
        all_scene_info = pd.merge(self.scene_info,self.data_provider[Data.ALL_TEMPLATES],on='template_fk')
        df = pd.merge(df, all_scene_info, on="scene_fk")
        df = df[self.tools.get_filter_condition(df, **filters)]
        df = df.groupby(['template_name', 'scene_fk']).size().reset_index(name='num_packs')
        return df

    def count_of_scenes(self, df, filters):
        facing_data = df[self.tools.get_filter_condition(df, **filters)]

        # filter by scene_id and by template_name (scene type)
        scene_types_groupby = facing_data.groupby(['template_name', 'scene_id'])['facings'].sum().reset_index()
        return scene_types_groupby


    def count_of_scenes_facings(self, df, filters):
        all_scene_info = pd.merge(self.scene_info,self.data_provider[Data.ALL_TEMPLATES],on='template_fk')
        df = pd.merge(df, all_scene_info, on="scene_fk")
        df = df[self.tools.get_filter_condition(df, **filters)]
        df['face_count'] = df['face_count'].fillna(1)
        df = df.groupby(['template_name', 'scene_fk'])['face_count'].sum().reset_index()
        return df

    def handle_survey_atomics(self, atomic_id, atomic_name):
        # bring the kpi rows from the survey sheet
        row = self.survey_sheet.loc[self.survey_sheet[Const.KPI_ID] == atomic_id]

        if row.empty:
            return
        else:
            # find the answer to the survey in session
            question_id = row[Const.SURVEY_QUESTION_ID].values[0]
            question_answer_template = row[Const.TARGET_ANSWER].values[0]

            survey_result = self.survey.get_survey_answer(('question_fk', question_id))
            if not survey_result:
                return
            if question_answer_template == Const.NUMERIC:
                if not isinstance(survey_result, (int, long, float)):
                    Log.warning("question id " + str(question_id) + " in template is not a number")
                    return

            else:
                answer = self.survey.check_survey_answer(('question_fk', question_id), question_answer_template)
                survey_result = 1 if answer else -1

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_type(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=self.session_id, numerator_result=0,
                                       denominator_result=0, denominator_id=1, result=survey_result)

    def handle_prod_seq_atomics(self, atomic_id, atomic_name, parent_kpi):
        result = 0

        # bring the kpi rows in the PROD_SEQ sheet
        rows = self.prod_seq_sheet.loc[self.prod_seq_sheet[Const.KPI_ID] == atomic_id]

        # get a the correct rows
        temp = rows[Const.STORE_TYPE_TEMPLATE]
        filtered_rows = rows[(temp.apply(lambda r: self.store_type_filter in r.split(","))) | (temp == "")]
        if filtered_rows.empty:
            return
        temp_row = filtered_rows.iloc[0]
        del temp_row[Const.SUB_CATEGORY]
        del temp_row[Const.LEFT_RIGHT_SUBCATEGORY]
        filters = self.get_filters_from_row(temp_row.squeeze())
        scenes = self.get_scene_list(filters)
        matches = self.match_product_in_scene.copy()
        matches = matches[matches['scene_fk'].isin(scenes)]
        matches_merged = pd.merge(matches, self.all_products, how='left', on='product_fk').fillna("")
        matches_merged_ns = matches_merged[matches_merged['stacking_layer'] == 1]
        shelfs_to_iterate = matches_merged_ns[matches_merged_ns['sub_category'].isin(filtered_rows[
                            Const.SUB_CATEGORY].tolist())][['scene_fk','bay_number','shelf_number']].drop_duplicates()
        denominator_shelfs = len(shelfs_to_iterate)
        numerator_shelfs = 0
        for i, s in shelfs_to_iterate.iterrows():
            working_shelf = matches_merged_ns[(matches_merged_ns['scene_fk'] == s['scene_fk'])
                                                      & (matches_merged_ns['bay_number'] == s['bay_number'])
                                                      & (matches_merged_ns['shelf_number'] == s['shelf_number'])]
            df = working_shelf.sort_values('facing_sequence_number')['sub_category']
            df = df.loc[df.shift(-1) != df]
            list_df = df.tolist()
            num_of_products = len(list_df)
            if (num_of_products < 2):
                numerator_shelfs += 1
                continue
            check_result = self.check_order_prod_seq(list_df,filtered_rows,num_of_products)
            if check_result:
                numerator_shelfs += 1

        target = temp_row[Const.TARGET]
        weight = temp_row[Const.WEIGHT]
        if ((denominator_shelfs != 0) and ((numerator_shelfs / denominator_shelfs) * 100 >= target)):
            result = weight

        if result == 0:
            return

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_type(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.parent_kpis[parent_kpi] += result


        self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, identifier_parent=parent_kpi,
                                        denominator_result_after_actions=1,
                                        numerator_result=numerator_shelfs, denominator_id=self.store_fk,
                                        should_enter=True, denominator_result=denominator_shelfs, result=result)

    def check_order_prod_seq(self, list_df, filtered_rows, num_of_products):
        shelf_fail = False
        for j, sub_cat_row in filtered_rows.iterrows():
            if shelf_fail:
                break
            if sub_cat_row[Const.SUB_CATEGORY] not in list_df:
                continue
            adjacent_subscategories = sub_cat_row[Const.LEFT_RIGHT_SUBCATEGORY].split(",")
            adjacent_subscategories = [item.strip() for item in adjacent_subscategories]
            for k in range(0, num_of_products):
                if (list_df[k] in sub_cat_row[Const.SUB_CATEGORY]):
                    if k == 0:
                        if list_df[1] not in adjacent_subscategories:
                            shelf_fail = True
                            break
                    elif k == (num_of_products - 1):
                        if list_df[num_of_products - 2] not in adjacent_subscategories:
                            shelf_fail = True
                            break
                    elif ((list_df[k - 1] not in adjacent_subscategories) and
                          (list_df[k + 1] not in adjacent_subscategories)):
                        shelf_fail = True
                        break
        if not shelf_fail:
            return  True
        return False

    def handle_prod_seq_2_atomics(self, atomic_id, atomic_name, parent_kpi):

        count_result = 0
        result = 0

        # bring the kpi rows in the PROD_SEQ_2 sheet
        rows = self.prod_seq_2_sheet.loc[self.prod_seq_2_sheet[Const.KPI_ID] == atomic_id]

        # get a the correct rows
        temp = rows[Const.STORE_TYPE_TEMPLATE]
        matching_row = rows[(temp.apply(lambda r: self.store_type_filter in r.split(","))) | (temp == "")]
        groups_outside = matching_row[Const.BRAND_GROUP_OUTSIDE].values[0].split(',')
        groups_inside = matching_row[Const.BRAND_GROUP_INSIDE].values[0].split(',')

        del matching_row[Const.BRAND_GROUP_OUTSIDE]
        del matching_row[Const.BRAND_GROUP_INSIDE]

        filters = self.get_filters_from_row(matching_row.squeeze())

        scenes = self.get_scene_list(filters)

        matches = self.match_product_in_scene.copy()
        matches = matches[matches['scene_fk'].isin(scenes)]
        matches_merged = pd.merge(matches, self.all_products, how='left', on='product_fk').fillna(0)
        matches_merged_ns = matches_merged[matches_merged['stacking_layer'] == 1]
        filtered_shelfs = matches_merged_ns[matches_merged_ns['brand_name'].isin(groups_inside)][['scene_fk',
                                                                    'bay_number', 'shelf_number']].drop_duplicates()
        denominator_shelfs = len(filtered_shelfs)
        numerator_shelfs = 0
        for i,row in filtered_shelfs.iterrows():
            working_shelf = matches_merged_ns[(matches_merged_ns['scene_fk'] == row['scene_fk']) & (matches_merged_ns[
                      'bay_number'] == row['bay_number']) & (matches_merged_ns['shelf_number'] == row['shelf_number'])]
            if len(groups_inside) == 1:
                check = True
                for g in groups_outside:
                    if len(working_shelf[working_shelf['brand_name'] == g]) == 0:
                        check = False
                if check == False:
                    continue
            else:
                if len(working_shelf[working_shelf['brand_name'].isin(groups_outside)]) == 0:
                    continue

            result = self.check_order_prod_seq_2(working_shelf, groups_outside, groups_inside)
            if result:
                numerator_shelfs += 1

        target = matching_row[Const.TARGET].values[0]
        weight = matching_row[Const.WEIGHT].values[0]
        if ((denominator_shelfs != 0) and ((numerator_shelfs / denominator_shelfs) * 100 >= target)):
            count_result = weight

        if result == 0:
            return

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_type(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.parent_kpis[parent_kpi] += count_result

        self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, identifier_parent=parent_kpi,
                                          denominator_result_after_actions=1,
                                          numerator_result=numerator_shelfs, denominator_id=self.store_fk,
                                          should_enter=True, denominator_result=denominator_shelfs, result=count_result)

    def check_order_prod_seq_2(self, working_shelf, groups_outside , groups_inside):
        result = False
        df = working_shelf.sort_values('facing_sequence_number')['brand_name']

        # drop adjacent brand duplicates
        df = df.loc[df.shift(-1) != df]
        list_df = df.tolist()
        list_df = [str(item) for item in list_df]
        str_df = "".join(list_df)
        constraints = self.calc_constraints(groups_outside, groups_inside, list_df)
        for c in constraints:
            if c in str_df:
                result = True
                str_df = str_df.replace(c,"",1)
                for i in groups_inside:
                    result = result and (i in str_df)
                break
        return result

    def calc_constraints(self,groups_outside, groups_inside, list_df):
        a = groups_outside[0]
        b = groups_outside[1]
        c = groups_inside[0]

        if (a in list_df and b in list_df):
            if len(groups_inside) > 1:
                d = groups_inside[1]
                constraints = [a+c+d+b,a+d+c+b,b+d+c+a,b+c+d+a] + [a+c+b,a+d+b,b+c+a,b+d+a]
            else:
                constraints = [a+c+b,b+c+a]
        else:
            if (b in list_df):
                a = b
            if len(groups_inside) > 1:
                d = groups_inside[1]
                constraints = [a+c+d,a+d+c,d+c+a,c+d+a,c+a+d,d+a+c] + [a+c,a+d,c+a,d+a]
            else:
                constraints = [a+c,c+a]

        return constraints


    def get_scene_list(self, filters):
        scenes_list = self.scif[self.tools.get_filter_condition(self.scif, **filters)]['scene_fk'].unique().tolist()
        return scenes_list

    def handle_prod_weight_atomics(self, atomic_id, atomic_name, parent_kpi):
        total_weight = 0
        total_facings = 0
        limit = 100

        # bring the kpi rows in the PROD_SEQ_2 sheet
        rows_sku = self.prod_weight_sku_sheet.loc[self.prod_weight_sku_sheet[Const.KPI_ID] == atomic_id]
        rows_subbrand = self.prod_weight_subbrand_sheet.loc[self.prod_weight_subbrand_sheet[Const.KPI_ID] == atomic_id]

        matching_rows_sku = rows_sku[rows_sku[Const.STORE_TYPE_TEMPLATE] == self.store_type_filter]
        matching_rows_sku = matching_rows_sku[[Const.WEIGHT, Const.LIMIT_SCORE,self.state_name_filter]]
        matching_rows_sku = matching_rows_sku[matching_rows_sku[self.state_name_filter] != ""]

        matching_rows_subbrand = rows_subbrand[rows_subbrand[Const.STORE_TYPE_TEMPLATE] == self.store_type_filter]
        matching_rows_subbrand = matching_rows_subbrand[[Const.WEIGHT, Const.LIMIT_SCORE, self.state_name_filter]]
        matching_rows_subbrand = matching_rows_subbrand[matching_rows_subbrand[self.state_name_filter] != ""]

        if len(matching_rows_sku) > 0:
            limit = matching_rows_sku.iloc[0][Const.LIMIT_SCORE]
        elif len(matching_rows_subbrand) > 0:
            limit = matching_rows_sku.iloc[0][Const.LIMIT_SCORE]


        for i, row in matching_rows_sku.iterrows():
            product = row[self.state_name_filter]
            product_facings = self.scif[self.scif['product_name'] == product]['facings'].sum()
            total_facings += product_facings
            if product_facings > 0:
                total_weight += row[Const.WEIGHT]

        if 'sub_brand' in self.scif.columns:
            for i, row in matching_rows_subbrand.iterrows():
                subbrand = row[self.state_name_filter]
                subbrand_facings = self.scif[self.scif['sub_brand'] == subbrand]['facings'].sum()
                total_facings += subbrand_facings
                if subbrand_facings > 0:
                    total_weight += row[Const.WEIGHT]

        if limit < total_weight:
            total_weight = limit

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_type(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.parent_kpis[parent_kpi] += total_weight

        self.common_db.write_to_db_result(fk=atomic_pk, numerator_id=1, identifier_parent=parent_kpi,
                                        numerator_result=total_facings, denominator_id=self.store_fk,
                                        denominator_result_after_actions=2,
                                        should_enter=True, denominator_result=0, result=total_weight)

    def write_parent_kpis_results(self):
        self.common_db.write_to_db_result(fk=1, numerator_id=1, identifier_result=Const.CERVEJA,
                                            numerator_result=0, denominator_id=self.store_fk,
                                            denominator_result_after_actions=1,
                                            denominator_result=0, result=self.parent_kpis[Const.CERVEJA])
        self.common_db.write_to_db_result(fk=2, numerator_id=1, identifier_result=Const.GAME_PLAN,
                                            numerator_result=0, denominator_id=self.store_fk,
                                            denominator_result_after_actions=1,
                                            denominator_result=0, result=self.parent_kpis[Const.GAME_PLAN])
        self.common_db.write_to_db_result(fk=3, numerator_id=1, identifier_result=Const.NAB,
                                            numerator_result=0, denominator_id=self.store_fk,
                                            denominator_result_after_actions=1,
                                            denominator_result=0, result=self.parent_kpis[Const.NAB])
