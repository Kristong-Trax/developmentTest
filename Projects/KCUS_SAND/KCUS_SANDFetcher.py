# -*- coding: utf-8 -*-
import pandas as pd

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Projects.KCUS_SAND.Utils.KCUS_SANDJSON_2 import KCUS_SANDJson_2Generator

__author__ = 'ortalk'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
MARS = 'Mars'
OTHER = 'Other'

VERTEX_FK_FIELD = 'scene_match_fk'


class KCUS_SANDFetcher:
    TOP = 'shelf_px_top'
    BOTTOM = 'shelf_px_bottom'
    LEFT = 'shelf_px_left'
    RIGHT = 'shelf_px_right'

    def __init__(self, project_name, scif, matches, set_name, products, session_uid):
        self.rds_conn = AwsProjectConnector(project_name, DbUsers.CalculationEng)
        self.scif = scif
        self.matches = matches
        self.set_name = set_name
        self.kpi_static_data = self.get_static_kpi_data()
        self.products = products
        self.session_uid = session_uid

    def get_object_facings(self, scenes, objects, object_type, formula, form_factor=[], shelves=None,
                           brand_category=None, sub_brands=[], sub_brands_to_exclude=[], include_stacking=False, form_factor_to_exclude=[]):
        object_type_conversion = {'SKUs': 'product_ean_code',
                                  'BRAND': 'brand_name',
                                  'BRAND in CAT': 'brand_name',
                                  'CAT': 'category',
                                  'MAN in CAT': 'category',
                                  'MAN': 'manufacturer_name'}
        object_field = object_type_conversion[object_type]
        if object_type == 'MAN in CAT':
            initial_result = \
                self.scif.loc[
                    (self.scif['scene_id'].isin(scenes)) & (self.scif[object_field].isin(objects)) & (
                        self.scif['facings'] > 0) & (self.scif['rlv_dist_sc'] == 1) & (
                        self.scif['manufacturer_name'] == MARS) & (~self.scif['product_type'].isin([OTHER]))]
            merged_dfs = initial_result.merge(self.matches, on=['product_fk', 'scene_fk'])
            merged_filter = merged_dfs.loc[merged_dfs['stacking_layer'] == 1]
            final_result = merged_filter.drop_duplicates(subset='product_fk')
        elif object_type == 'BRAND in CAT':
            initial_result = \
                self.scif.loc[
                    (self.scif['scene_id'].isin(scenes)) & (self.scif[object_field].isin(objects)) & (
                        self.scif['facings'] > 0) & (self.scif['rlv_dist_sc'] == 1) & (
                        self.scif['category'] == brand_category) & (~self.scif['product_type'].isin([OTHER]))]
            merged_dfs = initial_result.merge(self.matches, on=['product_fk', 'scene_fk'])
            merged_filter = merged_dfs.loc[merged_dfs['stacking_layer'] == 1]
            final_result = merged_filter.drop_duplicates(subset='product_fk')
        else:
            initial_result = \
                self.scif.loc[
                    (self.scif['scene_id'].isin(scenes)) & (self.scif[object_field].isin(objects)) & (
                        self.scif['facings'] > 0) & (self.scif['rlv_dist_sc'] == 1) & (
                        ~self.scif['product_type'].isin([OTHER]))]
            merged_dfs = initial_result.merge(self.matches, how='left', on=['product_fk', 'scene_fk'])
            merged_filter = merged_dfs.loc[merged_dfs['stacking_layer'] == 1]
            final_result = merged_filter.drop_duplicates(subset='product_fk')
        if include_stacking:
            # merged_dfs = pd.merge(final_result, self.matches, on=['product_fk', 'product_fk'])
            # merged_filter = merged_dfs.loc[~merged_dfs['stacking_layer'] == 1]
            # final_result = merged_filter
            final_result = initial_result
        if form_factor:
            final_result = final_result[final_result['form_factor'].isin(form_factor)]
        if form_factor_to_exclude:
            final_result = final_result[~final_result['form_factor'].isin(form_factor_to_exclude)]
        # if size:
        #     final_result = final_result[final_result['size'].isin(size)]
        if shelves:
            merged_dfs = pd.merge(final_result, self.matches, on=['product_fk', 'product_fk'])
            shelves_list = [int(shelf) for shelf in shelves.split(',')]
            merged_filter = merged_dfs.loc[merged_dfs['shelf_number_x'].isin(shelves_list)]
            final_result = merged_filter
        if sub_brands:
            final_result = final_result[final_result['sub_brand_name'].isin(sub_brands)]
        if sub_brands_to_exclude:
            final_result = final_result[~final_result['sub_brand_name'].isin(sub_brands_to_exclude)]

        try:
            if "number of SKUs" in formula:
                object_facings = len(final_result['product_ean_code'].unique())
            else:
                if not include_stacking:
                    object_facings = final_result['facings_ign_stack'].sum()
                else:
                    object_facings = final_result['facings'].sum()
        except IndexError:
            object_facings = 0
        return object_facings

    @staticmethod
    def get_static_new_products():
        return  """
                   SELECT sp.*, cat.name as category, 
                    IF (labels like '%"FEM NEEDS": "Feminine Needs"%', 'Feminine Needs', null) as 'FEM NEEDS',
                    IF (labels like '%"FEM HYGINE": "Feminine Hygine"%', 'Feminine Hygiene',null) as 'FEM HYGINE'
                    
                    FROM static_new.product sp
                    left join static_new.category as cat 
                    on cat.pk = sp.category_fk ;
                
                """


    @staticmethod
    def get_kpi_results_data():

        return '''
            select api.pk as atomic_kpi_fk
            from
            static.atomic_kpi api
            left join static.kpi kpi on kpi.pk = api.kpi_fk
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
            where api.name = '{}' and kps.name = '{}'
            limit 1
                   '''

    @staticmethod
    def get_kpk_results_data():
        return '''

            select k.pk as kpi_fk
            from static.kpi k
            left join static.kpi_set kp on kp.pk = k.kpi_set_fk
            where k.display_text = '{}' and kp.name = '{}'
            limit 1
                       '''

    @staticmethod
    def get_kps_results_data():
        return '''
                select kps.pk
    from static.kpi_set kps
            where kps.name = '{}'
            limit 1
                       '''

    # def get_kpi_fk(self, kpi_name, kps_name):
    #     query = self.get_kpk_results_data().format(kpi_name, kps_name)
    #     level2 = pd.read_sql_query(query, self.rds_conn.db)
    #     kpi_fk = level2['kpi_fk']
    #
    #     return kpi_fk

    def get_atomic_fk(self, kpi_fk):
        query = self.get_kpi_results_data().format(kpi_fk)
        level3 = pd.read_sql_query(query, self.rds_conn.db)
        atomic_fk = level3['atomic_kpi_fk']
        return atomic_fk

    def get_kps_fk(self, kps_name):
        query = self.get_kpi_results_data().format(kps_name)
        level1 = pd.read_sql_query(query, self.rds_conn.db)
        kpi_set_fk = level1['kpi_set_fk']
        return kpi_set_fk

    # def get_category_target_by_region(self, category, store_id):
    #     store_type_dict = {'PoS 2017 - MT - Hypermarket': 'Hypermarket',
    #                        'PoS 2017 - MT - Supermarket': 'Supermarket',
    #                        'PoS 2017 - MT - Superette': 'Superette'}
    #     store_region_fk = self.get_store_region(store_id)
    #     branch_fk = self.get_store_branch(store_id)
    #     jg = JsonGenerator('ccru')
    #     jg.create_targets_json('MT Shelf facings_2017.xlsx')
    #     targets = jg.project_kpi_dict['cat_targets_by_region']
    #     final_target = 0
    #     for row in targets:
    #         if row.get('branch_fk') == branch_fk and row.get('region_fk') == store_region_fk \
    #                 and row.get('store_type') == store_type_dict.get(self.set_name):
    #             final_target = row.get(category)
    #         else:
    #             continue
    #     return final_target
    #
    # def get_store_region(self, store_id):
    #     query = """
    #             SELECT region_fk
    #             FROM static.stores ss
    #             WHERE ss.pk = {};
    #             """.format(store_id)
    #
    #     cur = self.rds_conn.db.cursor()
    #     cur.execute(query)
    #     res = cur.fetchall()[0]
    #
    #     return res[0]
    #
    # def get_store_branch(self, store_id):
    #     query = """
    #             SELECT branch_fk
    #             FROM static.stores ss
    #             WHERE ss.pk = {};
    #             """.format(store_id)
    #
    #     cur = self.rds_conn.db.cursor()
    #     cur.execute(query)
    #     res = cur.fetchall()[0]
    #
    #     return res[0]

    def get_session_set(self, session_uid):
        query = """
                select ss.pk , ss.additional_attribute_12
                from static.stores ss
                join probedata.session ps on ps.store_fk=ss.pk
                where ss.delete_date is null and ps.session_uid = '{}';
                """.format(session_uid)

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()

        df = pd.DataFrame(list(res), columns=['store_fk', 'additional_attribute_12'])

        return df

    @staticmethod
    def get_table_update_query(entries, table, condition):
        if table == 'report.kpi_results':
            entries_to_overwrite = ["score", "display_text", "kps_name", "kpi_fk", "result", "threshold",
                                    "calculation_time"]
        elif table == 'report.kpk_results':
            entries_to_overwrite = ["score"]
        elif table == 'report.kps_results':
            entries_to_overwrite = ["score_1", "kps_name"]
        else:
            entries_to_overwrite = ["score"]
        updated_values = []
        for key in entries.keys():
            if key in entries_to_overwrite:
                updated_values.append("{} = '{}'".format(key, entries[key][0]))

        query = "UPDATE {} SET {} WHERE {}".format(table, ", ".join(updated_values), condition)

        return query

    def get_atomic_kpi_fk_to_overwrite(self, session_uid, atomic_kpi_fk):
        query = """
                select pk
                from report.kpi_results
                where session_uid = '{}' and atomic_kpi_fk = {}
                limit 1""".format(session_uid, atomic_kpi_fk)

        df = pd.read_sql_query(query, self.rds_conn.db)
        try:
            return df['pk'][0]
        except IndexError:
            return None

    def get_kpi_fk_to_overwrite(self, session_uid, kpi_fk):
        query = """
                select pk
                from report.kpk_results
                where session_uid = '{}' and kpi_fk = {}
                limit 1""".format(session_uid, kpi_fk)

        df = pd.read_sql_query(query, self.rds_conn.db)
        try:
            return df['pk'][0]
        except IndexError:
            return None

    def get_kpi_set_fk_to_overwrite(self, session_uid, kpi_set_fk):
        query = """
                select *
                from report.kps_results
                where session_uid = '{}' and kpi_set_fk = '{}'
                limit 1""".format(session_uid, kpi_set_fk)

        df = pd.read_sql_query(query, self.rds_conn.db)
        if not df.empty:
            return True
        else:
            return None

    def get_match_product_in_probe_details(self, session_uid):
        query = """
                select mpip.product_fk, mpip.probe_fk, mpip.price, pp.scene_fk, pp.local_image_time
                from
                probedata.match_product_in_probe_details mpip
                join probedata.probe pp on pp.pk=mpip.probe_fk
                where pp.session_uid = '{}'
                """.format(session_uid)

        df = pd.read_sql_query(query, self.rds_conn.db)

        return df

    def get_object_price(self, scenes, objects, object_type, match_product_details):
        """

        """
        object_type_conversion = {'SKUs': 'product_ean_code',
                                  'BRAND': 'brand_name',
                                  'CAT': 'category',
                                  'BRAND in CAT': 'brand_name',
                                  'MAN in CAT': 'category',
                                  'MAN': 'manufacturer_name'}
        object_field = object_type_conversion[object_type]
        final_result = \
            self.scif.loc[
                (self.scif['scene_id'].isin(scenes)) & (self.scif[object_field].isin(objects)) & (
                    self.scif['facings'] > 0) & (self.scif['rlv_dist_sc'] == 1)]
        merged_dfs = pd.merge(final_result, match_product_details, on=['product_fk', 'product_fk'])
        merged_filter = merged_dfs.loc[merged_dfs['stacking_layer'] == 1]

        object_prices = merged_filter['price']
        if not object_prices.empty:
            final_object_prices = max(object_prices)
        else:
            final_object_prices = 0

        return final_object_prices

    @staticmethod
    def get_delete_session_results(session_uid):
        # queries = ["""delete from report.kps_results
        #                 where kps_name = '{}' and session_uid = '{}'""",
        #            """delete kpk from report.kpk_results kpk
        #                 join static.kpi kpi on kpk.kpi_fk = kpi.pk
        #                 join static.kpi_set kps on kpi.kpi_set_fk = kps.pk
        #                 where kps.name = '{}' and session_uid = '{}'""",
        #            """delete atomic_kpi from report.kpi_results atomic_kpi
        #                 join static.kpi kpi on atomic_kpi.kpi_fk = kpi.pk
        #                 join static.kpi_set kps on kpi.kpi_set_fk = kps.pk
        #                 where kps.name = '{}' and session_uid = '{}'"""]
        queries = ["delete from report.kps_results where session_uid = '{}';".format(session_uid),
                   "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                   "delete from report.kpi_results where session_uid = '{}';".format(session_uid)]
        return queries

    def get_kpi_set_fk(self):
        kpi_set_fk = self.kpi_static_data['kpi_set_fk']
        return kpi_set_fk.values[0]

    def get_kpi_fk(self, kpi_name):
        kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == str(kpi_name)]['kpi_fk']
        return kpi_fk.values[0]

    def get_atomic_kpi_fk(self, atomic_kpi_name):
        atomic_kpi_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == str(atomic_kpi_name)][
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
                where kps.name = '{}'""".format(self.set_name)
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_golden_shelves(self, shelves_num):
        jg = KCUS_SANDJson_2Generator('marsru')
        jg.create_targets_json('golden_shelves.xlsx', 'golden_shelves')
        targets = jg.project_kpi_dict['golden_shelves']
        final_shelves = []
        for row in targets:
            if row.get('num. of shelves min') <= shelves_num <= row.get('num. of shelves max'):
                start_shelf = row.get('num. ignored from top') + 1
                end_shelf = shelves_num - row.get('num. ignored from bottom')
                final_shelves = range(start_shelf, end_shelf + 1)
            else:
                continue
        return final_shelves

    def get_survey_answers_codes(self, survey_question_code, survey_answers_text):
        jg = KCUS_SANDJson_2Generator('marsru')
        jg.create_targets_json('answers_translation.xlsx', 'survey_answers_translation')
        targets = jg.project_kpi_dict['survey_answers_translation']
        answers_list = []
        for row in targets:
            if row.get('question code') == int(survey_question_code) and row.get('answer text') in survey_answers_text:
                answer_translation = row.get('answer translation')
                answers_list.append(answer_translation)
            else:
                continue
        final_answers = ','.join([str(result_value) for result_value in answers_list])

        return final_answers

    def get_must_range_skus_by_region_and_store(self, store_type, region, kpi_name):
        jg = KCUS_SANDJson_2Generator('marsru')
        jg.create_targets_json('MARS must-range targets.xlsx', 'must_range_skus', kpi_name)
        targets = jg.project_kpi_dict['must_range_skus']
        skus_list = []
        if store_type and region:  # Validation check
            for row in targets:
                store_types = str(row.get('Store type').encode('utf-8')).split(',\n')
                try:
                    regions = str(row.get('Region').encode('utf-8')).split(',\n')
                except AttributeError as e:
                    regions = None
                if regions:
                    if store_type.encode('utf-8') in store_types and region.encode('utf-8') in regions:
                        skus_list = str(row.get('EAN')).split(',\n')
                    else:
                        continue
                else:
                    if store_type.encode('utf-8') in store_types:
                        skus_list = str(row.get('EAN')).split(', ')
                    else:
                        continue
        return skus_list

    def get_filtered_matches(self, include_stacking=True):
        matches = self.matches
        matches = matches.sort_values(by=['bay_number', 'shelf_number', 'facing_sequence_number'])
        matches = matches[matches['status'] == 1]
        if not include_stacking:
            matches = matches[matches['stacking_layer'] == 1]
        matches = matches.merge(self.get_match_product_in_scene(), how='left', on='scene_match_fk')
        matches = matches.merge(self.products, how='left', on='product_fk')
        matches = matches.drop_duplicates(subset=[VERTEX_FK_FIELD])
        return matches

    def get_match_product_in_scene(self):
        query = """
                select ms.pk as scene_match_fk, ms.shelf_px_total, ms.n_shelf_items, ms.{}, ms.{}, ms.{}, ms.{}
                from probedata.match_product_in_scene ms
                join probedata.scene s on s.pk = ms.scene_fk
                where s.session_uid = '{}'""".format(self.TOP, self.BOTTOM, self.LEFT, self.RIGHT, self.session_uid)
        matches = pd.read_sql_query(query, self.rds_conn.db)
        return matches

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.kpi_set kps
            left join static.kpi kpi on kpi.kpi_set_fk = kps.pk
            left join static.atomic_kpi api on api.kpi_fk = kpi.pk
        """


    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_match_display(session_uid):
        return """
            select sdb.name, m.scene_fk, d.display_name, m.bay_number, m.rect_x, m.rect_y,dt.name as display_type
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            join static.display_brand sdb on sdb.pk=d.display_brand_fk
            join static.display_type dt on dt.pk = d.display_type_fk
            where s.session_uid = '{}'
        """.format(session_uid)


    @staticmethod
    def get_product_att4():
        return """
            SELECT att4,product_ean_code from
            static.product
        """