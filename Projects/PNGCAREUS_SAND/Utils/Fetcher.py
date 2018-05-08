
__author__ = 'Ortal'


class PNGCAREUSQueries(object):

    @staticmethod
    def get_all_kpi_data():
        return """
            select  kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.kpi_set kps
        """

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))
