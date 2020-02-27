# -*- coding: utf-8 -*-
import pandas as pd

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.CCRU.Utils.JSON import CCRUJsonGenerator
from Trax.Utils.Logging.Logger import Log


__author__ = 'urid'


KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

PRODUCT_STATIC_FIELDS = ['form_factor', 'size', 'product_ean_code', 'manufacturer_name', 'category',
                         'brand_name', 'sub_brand_name', 'product_type', 'product_fk', 'product_name']


class CCRUCCHKPIFetcher:

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

    def get_atomic_kpi_fk(self, atomic_kpi_name, kpi_fk=None):
        try:
            atomic_kpi_name = atomic_kpi_name.decode('utf-8')
        except UnicodeEncodeError:
            pass

        if kpi_fk:
            atomic_kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_fk'] == kpi_fk) &
                                                 (self.kpi_static_data['atomic_kpi_name'] ==
                                                  atomic_kpi_name.replace("\\'", "'"))]['atomic_kpi_fk']
        else:
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
        jg = CCRUJsonGenerator()
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
        result = cur.fetchall()

        try:
            result = float(result[0][0].replace(',', '.'))
        except:
            result = 1.0

        return result

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

    def get_session_user(self, session_uid):
        query = """
                SELECT 
                    sr.login_name AS user_name, 
                    ur.role AS user_role, 
                    sr.position AS user_position
                FROM probedata.session ss
                LEFT JOIN static.sales_reps sr ON sr.pk=ss.s_sales_rep_fk
                LEFT JOIN static.mobile_user_roles ur ON ur.sales_rep_fk=sr.pk
                WHERE ss.session_uid='{}';
                """.format(session_uid)
        result = pd.read_sql_query(query, self.rds_conn.db).to_dict(orient='records')[0]
        return result

    def get_planned_visit_flag(self, session_uid):
        query = """
                SELECT pv.planned_flag
                FROM probedata.session ss
                LEFT JOIN pservice.planned_visits pv ON 1
                AND pv.store_fk=ss.store_fk 
                AND pv.sales_rep_fk=ss.s_sales_rep_fk 
                AND pv.visit_date=ss.visit_date
                WHERE ss.session_uid='{}';
                """.format(session_uid)
        result = pd.read_sql_query(query, self.rds_conn.db)[0][0]
        return result

    def get_top_skus_for_store(self, store_fk, visit_date):
        query = """
                select
                anchor_product_fk,
                group_concat(product_fk) as product_fks,
                max(min_facings) as min_facings
                from (
                    select          
                    ifnull(ts.anchor_product_fk, ts.product_fk) as anchor_product_fk,
                    ts.product_fk as product_fk,
                    ifnull(ts.min_facings, 1) as min_facings
                    from {} ts
                    where ts.store_fk = {}
                    and ts.start_date <= '{}' 
                    and ifnull(ts.end_date, curdate()) >= '{}'
                ) t
                group by anchor_product_fk;
                """.format('pservice.custom_osa',
                           store_fk,
                           visit_date,
                           visit_date)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data.groupby(['anchor_product_fk']).agg({'product_fks': 'first', 'min_facings': 'first'}).to_dict()

    def get_custom_entity(self, entity_type):
        query = \
            """
            SELECT en.pk, en.name
            FROM static.custom_entity en
            JOIN static.kpi_entity_type et ON et.pk=en.entity_type_fk
            WHERE et.name = '{}';
            """.format(entity_type)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def get_kpi_level_2_fk(self, kpi_level_2_type):
        query = \
            """
            SELECT pk FROM static.kpi_level_2
            WHERE type = '{}';
            """.format(kpi_level_2_type)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return None if data.empty else data.values[0][0]

    def get_kpi_operation_type_fk(self, kpi_operation_type):
        query = \
            """
            SELECT pk FROM static.kpi_operation_type
            WHERE operation_type = '{}';
            """.format(kpi_operation_type)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return None if data.empty else data.values[0][0]

    def get_scene_item_prices(self, scene_list):
        query = """
                SELECT
                scene_fk,
                product_fk,
                MIN(IF((is_promotion = 0 AND outlier <> (-2) AND outlier <> 2) 
                        OR (is_promotion = 0 AND (ISNULL(outlier) OR outlier = 0)), price, NULL)) AS price
                FROM (
                    SELECT 
                     mpip.product_fk AS product_fk
                    ,mpip.probe_fk AS probe_fk
                    ,pr.scene_fk AS scene_fk
                    ,mpippav.value AS price
                    ,mpippav.is_promotion AS is_promotion
                    ,IF(ISNULL(tipr.soft_min) OR ISNULL(mpippav.value),NULL
                        ,IF(mpippav.value > tipr.soft_min AND mpippav.value <= tipr.soft_max, 0 
                            ,IF(mpippav.value > tipr.hard_min AND mpippav.value <= tipr.soft_min, -1
                                ,IF(mpippav.value > tipr.soft_max AND mpippav.value <= tipr.hard_max, 1
                                    ,IF(mpippav.value <= tipr.hard_min, -2
                                        ,2
                                        )
                                    )
                                )
                            )
                        ) 					 AS outlier
                    FROM probedata.match_product_in_probe  mpip
                    JOIN probedata.match_product_in_probe_price_attribute_value mpippav ON mpip.pk = mpippav.match_product_in_probe_fk
                    JOIN probedata.probe pr ON pr.pk = mpip.probe_fk AND ISNULL(pr.delete_time)
                    LEFT JOIN (
                        SELECT 
                             table1.pk
                            ,table1.product_fk 						AS product_fk
                            ,table1.soft_min 					 	AS soft_min
                            ,table1.soft_max 					    AS soft_max
                            ,table1.hard_min						AS hard_min
                            ,table1.hard_max						AS hard_max
                            ,table1.avg_price						AS avg_price
                            ,table1.creation_time 				 	AS start_date
                            ,IFNULL(table2.creation_time, NOW()) 	AS end_date
                        FROM (
                        SELECT ppr.*, @rownum := @rownum+1 AS rownum
                        FROM static.product_price_range ppr
                        JOIN (SELECT @rownum := 0) r
                        ORDER BY ppr.product_fk,ppr.creation_time
                        ) table1
                        LEFT JOIN (
                        SELECT ppr.*, @rownum2 := @rownum2+1 AS rownum2
                        FROM static.product_price_range ppr
                        JOIN (SELECT @rownum2 := -1) r
                        ORDER BY ppr.product_fk,ppr.creation_time
                        ) table2 ON table1.product_fk = table2.product_fk AND table1.rownum = table2.rownum2
                    ) tipr ON tipr.product_fk = mpip.product_fk AND (mpip.creation_time BETWEEN tipr.start_date AND tipr.end_date)
                    WHERE 1
                    AND pr.scene_fk IN ({scene_list})
                ) prices
                GROUP BY scene_fk, product_fk
                HAVING price IS NOT NULL;
                """.format(scene_list=','.join([unicode(x) for x in scene_list]))
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def get_scene_survey_response(self, scenes_list):
        if len(scenes_list) == 1:
            query = """
                       select sr.*, sq.question_text, sq.group_name 
                       from probedata.scene_survey_response sr
                       join static.survey_question sq on sr.question_fk=sq.pk
                       where sq.delete_time is null
                       and sq.group_name in ('Cooler Audit', 'Cooler Audit Test')
                       and sr.delete_time is null
                       and sr.scene_fk in ({});
                       """.format(scenes_list[0])
        else:
            query = """
                       select sr.*, sq.question_text, sq.group_name 
                       from probedata.scene_survey_response sr
                       join static.survey_question sq on sr.question_fk=sq.pk
                       where sq.delete_time is null
                       and sq.group_name in ('Cooler Audit', 'Cooler Audit Test')
                       and sr.delete_time is null
                       and sr.scene_fk in {};
                       """.format(tuple(scenes_list))
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def get_all_coolers_from_assortment_list(self, cooler_list):
        cooler_list = list(map(str, cooler_list))
        if len(cooler_list) == 1:
            query = """
                       select c.pk as cooler_fk, c.cooler_id, c.cooler_model_fk, m.name as cooler_model_name 
                       from pservice.cooler c
                       left join pservice.cooler_model m
                       on c.cooler_model_fk = m.pk
                       where c.cooler_id = '{}';
                       """.format(cooler_list[0])
        else:
            query = """
                       select c.pk as cooler_fk, c.cooler_id, c.cooler_model_fk, m.name as cooler_model_name 
                       from pservice.cooler c
                       left join pservice.cooler_model m
                       on c.cooler_model_fk = m.pk
                       where c.cooler_id in {};
                       """.format(tuple(cooler_list))
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def get_kpi_external_targets(self, visit_date, store_fk):
        query = """SELECT ext.*, ot.operation_type from static.kpi_external_targets ext
                   LEFT JOIN static.kpi_operation_type ot on ext.kpi_operation_type_fk=ot.pk 
                   WHERE 
                   ((ext.start_date<='{}' and ext.end_date is null) or 
                   (ext.start_date<='{}' and ext.end_date>='{}'))
                   AND ot.operation_type='COOLER_AUDIT'
                   AND ext.key_fk={}
                """.format(visit_date, visit_date, visit_date, store_fk)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data
