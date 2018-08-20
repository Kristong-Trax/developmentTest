import pandas as pd

__author__ = 'Shani'


class CUBAUCUBAUQueries(object):

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
    def get_store_attribute_9(store_fk):
        return """
                select additional_attribute_9 from static.stores
                where pk = {}
                """.format(store_fk)

    @staticmethod
    def get_facings_by_direction(session_uid):
        return """
                select m.pk as match_product_fk, image_direction from probedata.match_product_in_scene m
                join probedata.match_product_in_probe mp on mp.pk=m.probe_match_fk
                join static_new.product_image pi on pi.pk=mp.product_image_fk
                join probedata.scene sc on sc.pk=m.scene_fk
                join probedata.session se on se.session_uid=sc.session_uid
                join static.template t on t.pk=sc.template_fk
                where se.session_uid='{}'
                """.format(session_uid)
