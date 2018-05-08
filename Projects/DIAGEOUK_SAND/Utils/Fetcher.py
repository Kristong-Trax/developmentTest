
__author__ = 'Nimrod'


class DIAGEOUK_SANDQueries:

    def __init__(self):
        pass

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
    def get_business_unit_data(store_fk):
        return """
            select bu.name
            from static.stores st
            join static.business_unit bu on bu.pk = st.business_unit_fk
            where st.pk = {}
        """.format(store_fk)

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_match_display(session_uid):
        return """
            select d.display_name
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            where s.session_uid = '{}'
        """.format(session_uid)

