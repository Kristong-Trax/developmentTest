
__author__ = 'Nimrod'


class PNGAU_SANDQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.kpi_set kps
            join static.kpi kpi on kps.pk = kpi.kpi_set_fk
        """

    @staticmethod
    def get_match_display(session_uid):
        return """
            select d.display_name, m.scene_fk
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            where s.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_retailer(store_id):
        return """
            select r.name as retailer
            from static.stores st
            join static.retailer r on r.pk = st.retailer_fk
            where st.pk = {}""".format(store_id)

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))
