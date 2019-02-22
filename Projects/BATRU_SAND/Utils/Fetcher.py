
__author__ = 'uri'


class BATRU_SANDQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk, api.model_id as section
            from static.kpi_set kps
            left join static.kpi kpi on kps.pk = kpi.kpi_set_fk
            left join static.atomic_kpi api on kpi.pk = api.kpi_fk
        """

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_store_data(store_fk):
        return """
                SELECT s.pk as store_fk, s.store_number_1, additional_attribute_3
                FROM static.stores s
                WHERE s.pk = '{}'
        """.format(store_fk)

    @staticmethod
    def get_state(store_id):
        return """
            select st.name as state from static.stores s join static.state st on s.state_fk=st.pk where s.pk = {}
        """.format(store_id)

    @staticmethod
    def get_match_display(session_uid):
        return """
            select sdb.name, m.scene_fk, d.display_name, m.bay_number, m.rect_x, m.rect_y
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            join static.display_brand sdb on sdb.pk=d.display_brand_fk
            where s.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_templates_data():
        return """
            select key_json, data_json, start_date, end_date from static.kpi_external_targets where end_date is null
        """