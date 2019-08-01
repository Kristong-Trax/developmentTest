
__author__ = 'Nimrod'


class MARSINQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.presentation_order, api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.atomic_kpi api
            left join static.kpi kpi on kpi.pk = api.kpi_fk
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
            order by kpi_set_fk, kpi_fk, presentation_order
        """

    @staticmethod
    def get_missing_attributes_data():
        return """
            select pk as product_fk, att2
            from static.product
        """

    @staticmethod
    def get_match_display(session_uid):
        return """
            select d.display_name
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            where s.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))
