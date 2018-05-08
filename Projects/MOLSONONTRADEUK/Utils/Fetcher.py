
__author__ = 'nissand'


class MOLSONONTRADEUKQueries(object):

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
    def get_delete_session_results_query(session_uid, session_id):
        # return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
        #         "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
        #         "delete from report.kpi_results where session_uid = '{}';".format(session_uid),
        #         "delete from report.kpi_level_2_results where session_fk = '{}' and (kpi_level_2_fk "
        #         "between 300000 and 400000);".format(session_id))
        return ("delete from report.kpi_level_2_results where session_fk = '{}' and (kpi_level_2_fk "
                "in (select pk from static.kpi_level_2 where kpi_calculation_stage_fk = '3'));".format(session_id))

    @staticmethod
    def get_new_kpi_data():
        return """
            select *
            from static.kpi_level_2
        """

    @staticmethod
    def get_tap_type_data(session_uid):
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
