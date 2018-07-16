
__author__ = 'Nimrod'


class PEPSICOBRQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.description, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.atomic_kpi api
            left join static.kpi kpi on kpi.pk = api.kpi_fk
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
            order by api.presentation_order
        """

    @staticmethod
    def get_match_display(session_uid):
        return """
            select d.display_name, s.pk as scene_id
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            where s.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_prices(session_uid):
        return """select object_fk as product_fk, value as price
                  from probedata.action_report_response
                  where session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_store_data(store_fk):
        return """
                SELECT s.pk as store_fk , additional_attribute_10
                FROM static.stores s
                WHERE s.pk = '{}'
        """.format(store_fk)

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))
