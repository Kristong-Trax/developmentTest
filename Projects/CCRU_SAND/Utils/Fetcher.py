# -*- coding: utf-8 -*-
import pandas as pd

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.CCRU_SAND.Utils.JSON import CCRU_SANDJsonGenerator


__author__ = 'urid'


KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

PRODUCT_STATIC_FIELDS = ['form_factor', 'size', 'product_ean_code', 'manufacturer_name', 'category',
                         'brand_name', 'sub_brand_name', 'product_type', 'product_fk', 'product_name']


class CCRU_SANDCCHKPIFetcher:

    TCCC = ['TCCC', 'BF']

    def __init__(self, project_name):
        self.project_name = project_name
        self.rds_conn = self.rds_connection()
        self.kpi_set_name = None
        self.kpi_static_data = None

    def rds_connection(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    @staticmethod
    def get_delete_session_results(session_uid, session_fk):
        queries = ["delete from report.kps_results where session_uid = '{}';".format(session_uid),
                   "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                   "delete from report.kpi_results where session_uid = '{}';".format(session_uid),
                   "delete from pservice.custom_gaps where session_fk = '{}';".format(session_fk),
                   "delete from pservice.custom_scene_item_facts where session_fk = '{}';".format(session_fk)]
        return queries

    def get_static_kpi_data(self, kpi_set_name=None):
        kpi_set_name = kpi_set_name if kpi_set_name else self.kpi_set_name
        self.rds_conn.connect_rds()
        query = """
                select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                       kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                       kps.name as kpi_set_name, kps.pk as kpi_set_fk
                from static.atomic_kpi api
                join static.kpi kpi on kpi.pk = api.kpi_fk
                join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
                where kps.name = '{}'
                """.format(kpi_set_name)
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_kpi_set_fk(self):
        kpi_set_fk = self.kpi_static_data['kpi_set_fk']
        return kpi_set_fk.values[0]

    def get_kpi_fk(self, kpi_name):
        try:
            kpi_name = kpi_name.decode('utf-8')
        except UnicodeEncodeError:
            pass
        kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name.replace("\\'", "'")]['kpi_fk']
        if not kpi_fk.empty:
            return kpi_fk.values[0]
        else:
            return None

    def get_atomic_kpi_fk(self, atomic_kpi_name):
        try:
            atomic_kpi_name = atomic_kpi_name.decode('utf-8')
        except UnicodeEncodeError:
            pass
        atomic_kpi_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] ==
                                             atomic_kpi_name.replace("\\'", "'")]['atomic_kpi_fk']
        if not atomic_kpi_fk.empty:
            return atomic_kpi_fk.values[0]
        else:
            return None

    def get_category_target_by_region(self, category, store_id):
        store_type_dict = {'PoS 2017 - MT - Hypermarket': 'Hypermarket',
                           'PoS 2017 - MT - Supermarket': 'Supermarket',
                           'PoS 2017 - MT - Superette': 'Superette'}
        store_region_fk = self.get_store_region(store_id)
        branch_fk = self.get_store_branch(store_id)
        jg = CCRU_SANDJsonGenerator()
        jg.create_kpi_data_json('cat_targets_by_region', 'MT Shelf facings_2017.xlsx')
        targets = jg.project_kpi_dict['cat_targets_by_region']
        final_target = 0
        for row in targets:
            if row.get('branch_fk') == branch_fk and row.get('region_fk') == store_region_fk \
                    and row.get('store_type') == store_type_dict.get(self.set_name):
                final_target = row.get(category)
            else:
                continue
        return final_target

    def get_store_number(self, store_id):
        query = """
                SELECT store_number_1
                FROM static.stores ss
                WHERE ss.pk = {};
                """.format(store_id)
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()[0]
        return res[0]

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

    def get_attr15_store(self, store_id):
        query = """
                SELECT additional_attribute_15
                FROM static.stores ss
                WHERE ss.pk = {};
                """.format(store_id)
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()[0]
        if not all(res):
            return res[0]
        else:
            return float(res[0])

    def get_test_store(self, store_id):
        query = """
                SELECT test_store
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

    def get_external_session_id(self, session_uid):
        query = """
                SELECT external_session_id
                FROM probedata.session ss
                WHERE ss.session_uid = '{}';
                """.format(session_uid)
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()[0]
        return res[0]

    def get_store_info(self, store_id):
        query = """
                    SELECT s.pk as store_fk, s.additional_attribute_3, r.name as retailer, s.store_number_1,
                    s.business_unit_fk, s.additional_attribute_5, s.district_fk, s.test_store
                    FROM static.stores s
                    left join static.retailer r
                    on r.pk = s.retailer_fk where s.pk = '{}'
                """.format(store_id)
        store_info = pd.read_sql_query(query, self.rds_conn.db)
        return store_info

    def get_store_area_df(self, session_uid):
        query = """
                select sst.scene_fk, st.name, sc.session_uid from probedata.scene_store_task_area_group_items sst
                join static.store_task_area_group_items st on st.pk=sst.store_task_area_group_item_fk
                join probedata.scene sc on sc.pk=sst.scene_fk
                where sc.delete_time is null and sc.session_uid = '{}';
                """.format(session_uid)

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()
        df = pd.DataFrame(list(res), columns=['scene_fk', 'store_area_name', 'session_uid'])
        return df

    def get_kpi_result_values(self):
        self.rds_conn.connect_rds()
        query = """
                select 
                rt.pk as result_type_fk,
                rt.name as result_type, 
                rv.pk as result_value_fk, 
                rv.value as result_value
                from static.kpi_result_value rv
                join static.kpi_result_type rt on rt.pk=rv.kpi_result_type_fk;
                """
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_kpi_entity_types(self):
        self.rds_conn.connect_rds()
        query = """
                select * from static.kpi_entity_type;
                """
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_kpi_entity(self, entity, entity_type_fk, entity_table_name, entity_uid_field):
        self.rds_conn.connect_rds()
        query = """
                select 
                '{0}' as entity,
                {1} as type,
                pk as fk,
                {2} as uid_field
                from {3};
                """.format(entity, entity_type_fk, entity_uid_field, entity_table_name)
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df
