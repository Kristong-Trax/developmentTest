# -*- coding: utf-8 -*-
# import numpy as np
import pandas as pd

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log


__author__ = 'urid'


KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
MARS = 'Mars'
OTHER = 'Other'
EMPTY = 'Empty'
SKU = 'SKU'

VERTEX_FK_FIELD = 'scene_match_fk'


class MARSRU_PRODKPIFetcher:
    TOP = 'shelf_px_top'
    BOTTOM = 'shelf_px_bottom'
    LEFT = 'shelf_px_left'
    RIGHT = 'shelf_px_right'

    def __init__(self, project_name, kpi_templates, scif, matches, products, session_uid):
        self.rds_conn = PSProjectConnector(project_name, DbUsers.CalculationEng)
        self.project = project_name
        self.kpi_templates = kpi_templates
        self.scif = scif
        self.matches = matches
        self.kpi_static_data = self.get_static_kpi_data()
        self.kpi_result_values = self.get_kpi_result_values()
        self.products = products
        self.session_uid = session_uid

    def check_connection(self, rds_conn):
        try:
            rds_conn.db.cursor().execute(
                "select pk from probedata.session where session_uid = '{}';"
                    .format(self.session_uid))
        except:
            rds_conn.disconnect_rds()
            rds_conn.connect_rds()
            Log.debug('DB is reconnected')
            return False
        return True

    def get_object_facings(self, scenes, objects, object_type, formula,
                           manufacturers=None,
                           brands=None,
                           sub_brands=None,
                           sub_brands_to_exclude=None,
                           categories=None,
                           sub_cats=None,
                           sub_cats_to_exclude=None,
                           cl_sub_cats=None,
                           cl_sub_cats_to_exclude=None,
                           form_factors=None,
                           form_factors_to_exclude=None,
                           shelves=None,
                           include_stacking=False,
                           linear=False):

        object_type_conversion = {'MAN': 'manufacturer_name',
                                  'MAN in CAT': 'category',
                                  'BRAND': 'brand_name',
                                  'BRAND in CAT': 'brand_name',
                                  'CAT': 'category',
                                  'SKUs': 'product_ean_code',
                                  'SCENE_TYPE': 'template_name'}
        object_field = object_type_conversion[object_type]
        self.scif[object_field] = self.scif[object_field].str.encode('utf8')

        if not objects:
            objects = self.scif[object_field].unique().tolist()

        if not manufacturers:
            manufacturers = self.scif['manufacturer_name'].unique().tolist()

        if object_type == 'MAN in CAT':
            initial_result = self.scif.loc[(self.scif['scene_id'].isin(scenes)) &
                                           (self.scif[object_field].isin(objects)) &
                                           (self.scif['facings'] > 0) & (self.scif['rlv_dist_sc'] == 1) &
                                           (self.scif['manufacturer_name'].isin(manufacturers)) &
                                           (~self.scif['product_type'].isin([EMPTY]))]
        elif object_type == 'BRAND in CAT':
            if type(categories) is not list:
                categories = [categories]
            initial_result = self.scif.loc[(self.scif['scene_id'].isin(scenes)) &
                                           (self.scif[object_field].isin(objects)) &
                                           (self.scif['facings'] > 0) & (self.scif['rlv_dist_sc'] == 1) &
                                           (self.scif['category'].isin(categories)) &
                                           (~self.scif['product_type'].isin([EMPTY]))]
        else:
            initial_result = self.scif.loc[(self.scif['scene_id'].isin(scenes)) &
                                           (self.scif[object_field].isin(objects)) &
                                           (self.scif['facings'] > 0) & (self.scif['rlv_dist_sc'] == 1) &
                                           (~self.scif['product_type'].isin([EMPTY]))]

        if initial_result.empty:
            return 0

        merged_dfs = initial_result.merge(self.matches, how='left', on=['product_fk', 'scene_fk'], suffixes=['', '_1'])
        merged_filter = merged_dfs.loc[merged_dfs['stacking_layer'] == 1]
        final_result = merged_filter.drop_duplicates(subset=['product_fk', 'scene_fk'])

        if include_stacking:
            final_result = initial_result

        if manufacturers:
            final_result = final_result[final_result['manufacturer_name'].isin(manufacturers)]
        if brands:
            final_result = final_result[final_result['brand_name'].isin(brands)]
        if sub_brands:
            final_result = final_result[final_result['sub_brand'].isin(sub_brands)]
        if sub_brands_to_exclude:
            final_result = final_result[~final_result['sub_brand'].isin(sub_brands_to_exclude)]
        if categories:
            final_result = final_result[final_result['category'].isin(categories)]
        if sub_cats:
            final_result = final_result[final_result['Sub Category'].isin(sub_cats)]
        if sub_cats_to_exclude:
            final_result = final_result[~final_result['Sub Category'].isin(sub_cats_to_exclude)]
        if cl_sub_cats:
            final_result = final_result[final_result['Client Sub Category Name'].isin(cl_sub_cats)]
        if cl_sub_cats_to_exclude:
            final_result = final_result[~final_result['Client Sub Category Name'].isin(cl_sub_cats_to_exclude)]
        if form_factors:
            final_result = final_result[final_result['form_factor'].isin(form_factors)]
        if form_factors_to_exclude:
            final_result = final_result[~final_result['form_factor'].isin(form_factors_to_exclude)]
        if shelves:
            merged_dfs = pd.merge(final_result, self.matches, on=['product_fk'], suffixes=['', '_1'])
            shelves_list = [int(shelf) for shelf in shelves.split(',')]
            merged_filter = merged_dfs.loc[merged_dfs['shelf_number_x'].isin(shelves_list)]
            final_result = merged_filter

        try:
            if "number of SKUs" in formula:
                object_facings = len(final_result['product_ean_code'].unique())
            elif linear:
                if not include_stacking:
                    object_facings = final_result['gross_len_ign_stack'].sum()
                else:
                    object_facings = final_result['gross_len_split_stack'].sum()
            else:
                if not include_stacking:
                    object_facings = final_result['facings_ign_stack'].sum()
                else:
                    object_facings = final_result['facings'].sum()
        except IndexError:
            object_facings = 0

        return object_facings

    def get_object_price(self, scenes, objects, object_type, match_product_details, form_factors=None,
                         include_stacking=False):
        object_type_conversion = {'MAN': 'manufacturer_name',
                                  'MAN in CAT': 'category',
                                  'BRAND': 'brand_name',
                                  'BRAND in CAT': 'brand_name',
                                  'CAT': 'category',
                                  'SKUs': 'product_ean_code'}
        object_field = object_type_conversion[object_type]
        final_result = \
            self.scif.loc[(self.scif['scene_id'].isin(scenes)) &
                          (self.scif[object_field].isin(objects)) &
                          (self.scif['facings'] > 0) &
                          (self.scif['rlv_dist_sc'] == 1) &
                          (self.scif['form_factor'].isin(form_factors))]
        merged_dfs = pd.merge(final_result, match_product_details,
                              on=['product_fk'], suffixes=['', '_1'])
        if not include_stacking:
            merged_filter = merged_dfs.loc[merged_dfs['stacking_layer'] == 1]
        else:
            merged_filter = merged_dfs
        object_prices = merged_filter[['price', 'promotion_price']]

        if not object_prices.empty:
            if object_prices.values.max():
                final_object_prices = object_prices.values.max()
            else:
                final_object_prices = 0
        else:
            final_object_prices = 0

        return final_object_prices

    @staticmethod
    def get_delete_session_results(session_uid):
        queries = ["delete from report.kps_results where session_uid = '{}';".format(session_uid),
                   "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                   "delete from report.kpi_results where session_uid = '{}';".format(session_uid)]
        return queries

    @staticmethod
    def get_delete_session_custom_scif(session_fk):
        query = "delete from pservice.custom_scene_item_facts where session_fk = '{}';".format(
            session_fk)
        return query

    def get_kpi_set_fk(self, kpi_set_name):
        kpi_set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == str(kpi_set_name)]['kpi_set_fk']
        return kpi_set_fk.values[0]

    def get_kpi_fk(self, kpi_set_name, kpi_name):
        kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == str(kpi_set_name)) &
                                      (self.kpi_static_data['kpi_name'] == str(kpi_name))]['kpi_fk']
        return kpi_fk.values[0]

    def get_atomic_kpi_fk(self, kpi_set_name, kpi_name, atomic_kpi_name):
        atomic_kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == str(kpi_set_name)) &
                                             (self.kpi_static_data['kpi_name'] == str(kpi_name)) &
                                             (self.kpi_static_data['atomic_kpi_name'] == str(atomic_kpi_name))][
            'atomic_kpi_fk']
        return atomic_kpi_fk.values[0]

    def get_static_kpi_data(self):
        query = """
                select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                       kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                       kps.name as kpi_set_name, kps.pk as kpi_set_fk
                from static.atomic_kpi api
                join static.kpi kpi on kpi.pk = api.kpi_fk
                join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
                """
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_kpi_result_values(self):
        query = \
            """
            SELECT
            rv.pk AS kpi_result_value_fk,
            rv.value AS kpi_result_value,
            rv.kpi_result_type_fk,
            rt.name AS kpi_result_type,
            rt.kpi_scale_type_fk,
            st.scale_type AS kpi_scale_type
            FROM static.kpi_result_value rv
            JOIN static.kpi_result_type rt ON rt.pk=rv.kpi_result_type_fk
            JOIN static.kpi_scale_type st ON st.pk=rt.kpi_scale_type_fk;
            """
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_golden_shelves(self, shelves_num):
        targets = self.kpi_templates['golden_shelves']
        final_shelves = []
        for row in targets:
            if row.get('num. of shelves min') <= shelves_num <= row.get('num. of shelves max'):
                start_shelf = row.get('num. ignored from top') + 1
                end_shelf = shelves_num - row.get('num. ignored from bottom')
                final_shelves = range(start_shelf, end_shelf + 1)
            else:
                continue
        return final_shelves

    def get_survey_answers_translation(self, survey_question_code, survey_answers_text):
        targets = self.kpi_templates['survey_answers_translation']
        answers_list = []
        for row in targets:
            if row.get('question code') == int(survey_question_code) and row.get('answer text') in survey_answers_text:
                answer_translation = row.get('answer translation')
                answers_list.append(answer_translation)
            else:
                continue
        final_answers = ','.join([str(result_value) for result_value in answers_list])

        return final_answers

    def get_kpi_value_parameters(self, store_type, region, params, kpi_results):
        kpi_name = params.get('#Mars KPI NAME')
        kpi_type = params.get('Type')
        kpi_values = params.get('Values')

        targets = self.kpi_templates['range_targets'].get(kpi_name)

        store_type = store_type if store_type else ''
        region = region if region else ''

        values = {}
        eans = None
        shelves = None
        shelf_length = None

        if kpi_values:
            if kpi_type == 'SKU_LIST' and kpi_values:
                for row in self.kpi_templates['sku_lists']:
                    if row['SKU List'] == kpi_values:
                        eans = row['EAN']
                        break
            elif kpi_type == 'SKUs' and kpi_values:
                eans = kpi_values

        if targets:  # Validation check

            if kpi_name in [2254, 4254]:
                shelf_length = []
                for row in targets:
                    if region.encode('utf-8') != row.get('Region').encode('utf-8').strip() or \
                            store_type.encode('utf-8') != row.get('Store type').encode('utf-8').strip():
                        continue
                    try:
                        shelf_length_from = float(row.get('Shelf length FROM INCLUDING'))
                    except ValueError:
                        shelf_length_from = 0
                    try:
                        shelf_length_to = float(row.get('Shelf length TO EXCLUDING'))
                    except ValueError:
                        shelf_length_to = 10000
                    result = str(row.get('Result')).strip()
                    length_condition = str(row.get('Length condition')).strip()
                    shelf_length.append({
                                            'shelf from': shelf_length_from,
                                            'shelf to': shelf_length_to,
                                            'result': result,
                                            'length_condition': length_condition
                                        })

            else:

                if ('EAN' or 'SKU List') in targets[0] and not eans \
                        or 'Shelf # from the bottom' in targets[0] and not shelves:

                    for row in targets:

                        if 'Store type' in row:
                            store_types = str(row.get('Store type').encode(
                                'utf-8')).strip().replace('\n', '').split(',')
                        else:
                            store_types = []

                        if 'Region' in row:
                            regions = str(row.get('Region').encode(
                                'utf-8')).strip().replace('\n', '').split(',')
                        else:
                            regions = []

                        if (not store_types or store_type.encode('utf-8') in store_types) and\
                                (not regions or region.encode('utf-8') in regions):

                            if 'KPI name' in row:

                                kpi_name_to_check = str(row.get('KPI name')).encode('utf-8').strip()
                                kpi_results_to_check = str(row.get('KPI result'))\
                                    .encode('utf-8').strip().replace('\n', '').split(',')
                                kpi_result = str(kpi_results.get(kpi_name_to_check).get('result'))\
                                    if kpi_results.get(kpi_name_to_check) else None
                                if kpi_result:
                                    if kpi_result in kpi_results_to_check:
                                        if not eans and 'EAN' in row:
                                            eans = row['EAN']
                                        elif not eans and 'SKU List' in row:
                                            for sku_list_row in self.kpi_templates['sku_lists']:
                                                if sku_list_row['SKU List'] == row['SKU List']:
                                                    eans = sku_list_row['EAN']
                                                    break
                                        if not shelves and 'Shelf # from the bottom' in row:
                                            shelves = row['Shelf # from the bottom']
                                        break
                                    else:
                                        continue
                                else:
                                    continue

                            else:
                                if not eans and 'EAN' in row:
                                    eans = row['EAN']
                                elif not eans and 'SKU List' in row:
                                    for sku_list_row in self.kpi_templates['sku_lists']:
                                        if sku_list_row['SKU List'] == row['SKU List']:
                                            eans = row['SKU List']
                                            break
                                if not shelves and 'Shelf # from the bottom' in row:
                                    shelves = row['Shelf # from the bottom']
                                break
                        else:
                            continue

        if eans and type(eans) in (str, unicode):
            eans = [x.strip()
                    for x in str(eans).replace(',', '\n').replace('\n\n', '\n').replace('\n\n', '\n').split('\n')]
            values['eans'] = eans

        if shelves and type(shelves) in (str, unicode):
                values['shelves'] = shelves.strip()

        if shelf_length and type(shelf_length) in (list, tuple):
                values['shelf_length'] = shelf_length

        return values

    def get_filtered_matches(self, include_stacking=True):
        self.rds_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)
        matches = self.matches
        matches = matches.sort_values(by=['bay_number', 'shelf_number', 'facing_sequence_number'])
        matches = matches[(matches['status'] == 1) | (matches['status'] == 3)]  # include stacking
        if not include_stacking:
            matches = matches[matches['stacking_layer'] == 1]
        matches = matches.merge(self.get_match_product_in_scene(), how='left',
                                on='scene_match_fk', suffixes=['', '_1'])
        matches = matches.merge(self.products, how='left', on='product_fk', suffixes=['', '_1'])
        matches = matches.drop_duplicates(subset=[VERTEX_FK_FIELD])
        return matches

    def get_match_product_in_scene(self):
        self.check_connection(self.rds_conn)
        query = """
                select ms.pk as scene_match_fk, ms.shelf_px_total, ms.n_shelf_items, ms.{}, ms.{}, ms.{}, ms.{}
                from probedata.match_product_in_scene ms
                join probedata.scene s on s.pk = ms.scene_fk
                where s.session_uid = '{}'""".format(self.TOP, self.BOTTOM, self.LEFT, self.RIGHT, self.session_uid)
        matches = pd.read_sql_query(query, self.rds_conn.db)
        return matches

    def get_store_att5(self, store_fk):
        query = """
                select additional_attribute_5
                from static.stores
                where pk = {}""".format(store_fk)
        store_att = pd.read_sql_query(query, self.rds_conn.db)
        return store_att.values[0][0]

    def get_store_att6(self, store_fk):
        query = """
                select additional_attribute_6
                from static.stores
                where pk = {}""".format(store_fk)
        store_att = pd.read_sql_query(query, self.rds_conn.db)
        return store_att.values[0][0]

    def get_store_att10(self, store_fk):
        query = """
                select additional_attribute_10
                from static.stores
                where pk = {}""".format(store_fk)
        store_att = pd.read_sql_query(query, self.rds_conn.db)
        return store_att.values[0][0]

    def get_store_assortment(self, attribute, visit_date):
        self.check_connection(self.rds_conn)
        query = """
                select product_fk from pservice.custom_osa
                where store_fk={0} and start_date <= '{1}' and (end_date >= '{1}'  OR end_date is null)
                """.format(attribute, visit_date)
        assortments = pd.read_sql_query(query, self.rds_conn.db)
        return assortments['product_fk'].tolist()

    def get_osa_assortment_fks(self, kpi_name):
        query = """
                SELECT a.pk FROM pservice.assortment a
                JOIN static.kpi_level_2 k ON k.pk=a.kpi_fk
                WHERE k.type='{}';
                """.format(kpi_name)
        assortment_fks = pd.read_sql_query(query, self.rds_conn.db)['pk'].unique().tolist()
        return assortment_fks

    def get_relevant_assortment_group(self, assortment_groups):
        assortment_groups = tuple(assortment_groups)
        self.check_connection(self.rds_conn)
        query = """
                SELECT a.pk AS assortment_group_fk, p.policy AS policy
                FROM pservice.assortment a
                JOIN pservice.policy p ON p.pk=a.store_policy_group_fk
                WHERE a.pk IN {}                
                """.format(assortment_groups)
        assortment_groups = pd.read_sql_query(query, self.rds_conn.db)
        if not assortment_groups.empty:
            if not assortment_groups[assortment_groups['policy'].str.startswith('{"store_number_1":')].empty:
                assortment_group = assortment_groups[assortment_groups['policy'].str.startswith('{"store_number_1":')][
                    'assortment_group_fk'].values[0]
            elif not assortment_groups[assortment_groups['policy'].str.startswith('{"retailer_name":')].empty:
                assortment_group = assortment_groups[assortment_groups['policy'].str.startswith('{"retailer_name":')][
                    'assortment_group_fk'].values[0]
            elif not assortment_groups[assortment_groups['policy'] == '{}'].empty:
                assortment_group = assortment_groups[assortment_groups['policy'] == '{}'][
                    'assortment_group_fk'].values[0]
            elif not assortment_groups[assortment_groups['policy'].str.startswith('{"region_name":')].empty:
                assortment_group = assortment_groups[assortment_groups['policy'].str.startswith('{"region_name":')][
                    'assortment_group_fk'].values[0]
            else:
                assortment_group = 0
        else:
            assortment_group = 0

        return assortment_group

    def get_store_number_1(self, store_fk):
        query = """
                select store_number_1
                from static.stores
                where pk = {}""".format(store_fk)
        store_num_1 = pd.read_sql_query(query, self.rds_conn.db)
        return store_num_1.values[0][0]

    def get_scene_survey_responses(self, session_fk):
        query = """
                SELECT sr.scene_fk, sq.code, sr.selected_option_text, text_value, number_value
                FROM probedata.scene_survey_response sr
                JOIN static.survey_question sq ON sq.pk=sr.question_fk
                JOIN probedata.scene sc ON sc.pk=sr.scene_fk
                JOIN probedata.session ss ON ss.session_uid=sc.session_uid 
                WHERE sr.delete_time IS NULL AND ss.pk = {} """.format(session_fk)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data
