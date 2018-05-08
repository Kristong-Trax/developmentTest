
__author__ = 'Ortal'


class PNGAMERICAQueries(object):

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
    def get_sub_category_data():
        return """
                select name as sub_category, pk as sub_category_fk from
                static_new.sub_category
            """


    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_store_retailer(store_fk):
        return """
                    select R.name from static.stores S
                    join static.retailer R
                    on S.retailer_fk = R.pk
                    where S.pk =  {}
                    """.format(store_fk)

    @staticmethod
    def get_additional_display_data(session_uid):
        query = """
                select mpp.pk as match_product_in_probe_fk, p.pk as probe_fk, p.scene_fk, p.session_uid, mpp.product_fk, mppsv.match_product_in_probe_state_fk
                from probedata.match_product_in_probe as mpp
                join probedata.probe as p
                on p.pk = mpp.probe_fk
                left join probedata.match_product_in_probe_state_value as mppsv
                on mpp.pk = mppsv.match_product_in_probe_fk
                where p.session_uid = '{}';
            """
        return query.format(session_uid)