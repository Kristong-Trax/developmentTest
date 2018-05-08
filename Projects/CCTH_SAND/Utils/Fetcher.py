
__author__ = 'Nimrod'


class CCTH_SANDQueries(object):

    @staticmethod
    def get_all_kpi_data(set_name):
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.atomic_kpi api
            left join static.kpi kpi on kpi.pk = api.kpi_fk
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
            where kps.name = '{}'
        """.format(set_name)

    @staticmethod
    def get_kpi_fk(set_name):
        return """
                select kps.pk as kpi_set_fk
                from static.kpi_set kps
                where kps.name = '{}'
            """.format(set_name)

    @staticmethod
    def get_segmentation_and_region_data(store_id):
        return """
            select st.additional_attribute_2 as segmentation, r.name as region
            from static.stores st
            join static.regions r on r.pk = st.region_fk
            where st.pk = {}
        """.format(store_id)

    @staticmethod
    def get_delete_session_results_query(session_uid, session_fk):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid),
                "delete from pservice.custom_gaps where session_fk = '{}';".format(session_fk))


    @staticmethod
    def get_store_data(store_id):
        return """
                select * from static.stores
                where pk = {}
            """.format(store_id)
