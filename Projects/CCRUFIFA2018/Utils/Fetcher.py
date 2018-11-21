# -*- coding: utf-8 -*-
import pandas as pd

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

from Projects.CCRUFIFA2018.Utils.JSON import JsonGenerator

__author__ = 'urid'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class CCRUFIFA2018Queries:
    TCCC = ['TCCC', 'BF']

    def __init__(self, project_name, scif, matches, set_name):
        self.rds_conn = PSProjectConnector(project_name, DbUsers.CalculationEng)
        self.scif = scif
        self.matches = matches
        self.set_name = set_name
        self.kpi_static_data = self.get_static_kpi_data()

    def get_object_facings(self, scenes, objects, object_type, formula, size=[], form_factor=[],
                           shelves=None, products_to_exclude=[], form_factors_to_exclude=[]):
        object_type_conversion = {'SKUs': 'product_ean_code',
                                  'BRAND': 'brand_name',
                                  'CAT': 'category',
                                  'MAN in CAT': 'category',
                                  'MAN': 'manufacturer_name'}
        object_field = object_type_conversion[object_type]
        final_result = \
            self.scif.loc[
                (self.scif['scene_id'].isin(scenes)) & (self.scif[object_field].isin(objects)) &
                (~self.scif['product_ean_code'].isin(products_to_exclude)) &
                (~self.scif['form_factor'].isin(form_factors_to_exclude)) &
                (self.scif['manufacturer_name'].isin(self.TCCC)) &
                (self.scif['facings'] > 0) &
                (~self.scif['product_type'].isin(['Empty']))]

        if form_factor:
            final_result = final_result[final_result['form_factor'].isin(form_factor)]
        if size:
            final_result = final_result[final_result['size'].isin(size)]
        if shelves:
            matches = self.matches.loc[self.matches['scene_fk'].isin(scenes)]
            merged_dfs = pd.merge(final_result, matches, on=['product_fk', 'product_fk'])
            if ',' in [shelves]:
                shelves_list = [int(shelf) for shelf in shelves.split(',')]
            else:
                shelves_list = [int(shelves)]
            merged_filter = merged_dfs.loc[merged_dfs['shelf_number'].isin(shelves_list)]
            final_result = merged_filter

        try:
            if 'shelf_number' in final_result.columns:
                object_facings = len(final_result['product_ean_code'])
            elif "number of SKUs" in formula:
                object_facings = len(final_result['product_ean_code'].unique())
            else:
                object_facings = final_result['facings'].sum()
        except IndexError:
            object_facings = 0
        return object_facings

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

    # @staticmethod
    # def get_kpi_results_data():
    #
    #     return '''
    #         select api.pk as atomic_kpi_fk
    #         from
    #         static.atomic_kpi api
    #         left join static.kpi kpi on kpi.pk = api.kpi_fk
    #         join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
    #         where api.display_text = '{}' and kps.name = '{}'
    #         limit 1
    #                '''

    # @staticmethod
    # def get_kpk_results_data():
    #     return '''
    #
    #         select k.pk as kpi_fk
    #         from static.kpi k
    #         left join static.kpi_set kp on kp.pk = k.kpi_set_fk
    #         where k.display_text = '{}' and kp.name = '{}'
    #         limit 1
    #                    '''

    # @staticmethod
    # def get_kps_results_data():
    #     return '''
    #             select kps.pk
    # from static.kpi_set kps
    #         where kps.name = '{}'
    #         limit 1
    #                    '''

    def get_kpi_set_fk(self):
        kpi_set_fk = self.kpi_static_data['kpi_set_fk']
        return kpi_set_fk.values[0]

    def get_kpi_fk(self, kpi_name):
        try:
            kpi_name = kpi_name.decode('utf-8')
        except UnicodeEncodeError:
            pass
        kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name.replace("\\'", "'")]['kpi_fk']
        return kpi_fk.values[0]

    def get_atomic_kpi_fk(self, atomic_kpi_name):
        try:
            atomic_kpi_name = atomic_kpi_name.decode('utf-8')
        except UnicodeEncodeError:
            pass
        atomic_kpi_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] ==
                                             atomic_kpi_name.replace("\\'", "'")]['atomic_kpi_fk']
        return atomic_kpi_fk.values[0]

    # def get_atomic_fk(self, kpi_fk):
    #     query = self.get_kpi_results_data().format(kpi_fk)
    #     level3 = pd.read_sql_query(query, self.rds_conn.db)
    #     atomic_fk = level3['atomic_kpi_fk']
    #     return atomic_fk

    # def get_kps_fk(self, kps_name):
    #     query = self.get_kpi_results_data().format(kps_name)
    #     level1 = pd.read_sql_query(query, self.rds_conn.db)
    #     kpi_set_fk = level1['kpi_set_fk']
    #     return kpi_set_fk

    def get_category_target_by_region(self, category, store_id):
        store_type_dict = {'PoS 2017 - MT - Hypermarket': 'Hypermarket',
                           'PoS 2017 - MT - Supermarket': 'Supermarket',
                           'PoS 2017 - MT - Superette': 'Superette'}
        store_region_fk = self.get_store_region(store_id)
        branch_fk = self.get_store_branch(store_id)
        jg = JsonGenerator('ccru')
        jg.create_targets_json('MT Shelf facings_2017.xlsx')
        targets = jg.project_kpi_dict['cat_targets_by_region']
        final_target = 0
        for row in targets:
            if row.get('branch_fk') == branch_fk and row.get('region_fk') == store_region_fk \
                    and row.get('store_type') == store_type_dict.get(self.set_name):
                final_target = row.get(category)
            else:
                continue
        return final_target

    def get_store_region(self, store_id):
        query = """
                    SELECT region_fk
                    FROM static.stores ss
                    WHERE ss.pk = {};
                    """.format(store_id)

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()[0]

        return res[0]

    def get_store_branch(self, store_id):
        query = """
                    SELECT branch_fk
                    FROM static.stores ss
                    WHERE ss.pk = {};
                    """.format(store_id)

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()[0]

        return res[0]

        # @staticmethod
        # def get_table_update_query(entries, table, condition):
        #     if table == 'report.kpi_results':
        #         entries_to_overwrite = ["score", "display_text", "kps_name", "kpi_fk", "result", "threshold",
        #                                 "calculation_time"]
        #     elif table == 'report.kpk_results':
        #         entries_to_overwrite = ["score"]
        #     elif table == 'report.kps_results':
        #         entries_to_overwrite = ["score_1", "kps_name"]
        #     else:
        #         entries_to_overwrite = ["score"]
        #     updated_values = []
        #     for key in entries.keys():
        #         if key in entries_to_overwrite:
        #             updated_values.append("{} = '{}'".format(key, entries[key][0]))
        #
        #     query = "UPDATE {} SET {} WHERE {}".format(table, ", ".join(updated_values), condition)
        #
        #     return query

        # def get_atomic_kpi_fk_to_overwrite(self, session_uid, atomic_kpi_fk):
        #     query = """
        #             select pk
        #             from report.kpi_results
        #             where session_uid = '{}' and atomic_kpi_fk = '{}'
        #             limit 1""".format(session_uid, atomic_kpi_fk)
        #
        #     df = pd.read_sql_query(query, self.rds_conn.db)
        #     try:
        #         return df['pk'][0]
        #     except IndexError:
        #         return None

        # def get_kpi_fk_to_overwrite(self, session_uid, kpi_fk):
        #     query = """
        #             select pk
        #             from report.kpk_results
        #             where session_uid = '{}' and kpi_fk = '{}'
        #             limit 1""".format(session_uid, kpi_fk)
        #
        #     df = pd.read_sql_query(query, self.rds_conn.db)
        #     try:
        #         return df['pk'][0]
        #     except IndexError:
        #         return None

        # def get_kpi_set_fk_to_overwrite(self, session_uid, kpi_set_fk):
        #     query = """
        #             select *
        #             from report.kps_results
        #             where session_uid = '{}' and kpi_set_fk = '{}'
        #             limit 1""".format(session_uid, kpi_set_fk)
        #
        #     df = pd.read_sql_query(query, self.rds_conn.db)
        #     if not df.empty:
        #         return True
        #     else:
        #         return None
