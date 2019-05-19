
__author__ = 'ortalk'

KPI_NAMES = ['PNG_Empty', 'Total_Empty', 'PG_non-Empty_Facing', 'Total_non-Empty_Facing', 'PNG_Empty_Rate%',
             'Category_Empty_Rate%']


class PNGQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.kpi_set kps
            left join static.kpi kpi on kps.pk = kpi.kpi_set_fk
            left join static.atomic_kpi api on kpi.pk = api.kpi_fk
        """

    @staticmethod
    def get_session_categories_query():
        return """
            select sc.category_fk as category, sc.exclude_status_fk as status
            from probedata.session_category sc
            join probedata.session se on sc.session_fk = se.pk
            where se.session_uid = '{}'
        """

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_insert_kpi_query(set_fk, name):
        return """INSERT INTO static.kpi (kpi_set_fk, display_text)
                  VALUES ('{0}', '{1}');""".format(set_fk, name.encode('utf-8'))

    @staticmethod
    def get_insert_atomic_query(kpi_fk):
        queries = []
        for name in KPI_NAMES:
            queries.append("""
            INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text, presentation_order, display)
            VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk, name, name, name, 1, 'Y'))
        return queries

    @staticmethod
    def get_display_count_per_name():
        return """
            select d.display_name, count(*) AS 'display_count'
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            where s.session_uid = '{}' AND d.is_physical = 1 GROUP BY display_name;"""


    @staticmethod
    def get_ean_code():
        return """
            select ean_code
            from static_new.product
            where pk = '{}';"""