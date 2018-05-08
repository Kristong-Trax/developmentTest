
__author__ = 'Elyashiv'


class CCZAQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.atomic_kpi api
            left join static.kpi kpi on kpi.pk = api.kpi_fk
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
        """

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def getPlanogramByTemplateName(scene_fk):
        return """
            SELECT match_compliance_status
            FROM report.match_planogram_compliance
            where scene_fk = {}
            """.format(scene_fk)

    @staticmethod
    def get_attr3(session_uid):
        return """
            SELECT st.additional_attribute_3 FROM static.stores st join probedata.session se on
            st.pk = se.store_fk where se.session_uid = '{}'
            """.format(session_uid)
