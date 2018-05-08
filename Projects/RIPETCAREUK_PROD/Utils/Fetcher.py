
__author__ = 'Nimrod'


class MarsUkQueries(object):

    @staticmethod
    def get_all_kpi_data_by_set(set_name):
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.atomic_kpi api
            left join static.kpi kpi on kpi.pk = api.kpi_fk
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
            where kps.name = '{}';
        """.format(set_name)

    @staticmethod
    def get_all_kpi_data():
        return """
                select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                       kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                       kps.name as kpi_set_name, kps.pk as kpi_set_fk
                from static.atomic_kpi api
                right join static.kpi kpi on kpi.pk = api.kpi_fk
                right join static.kpi_set kps on kps.pk = kpi.kpi_set_fk;"""

    @staticmethod
    def get_kpi_set_data():
        return """
                select
                       kps.name as kpi_set_name, kps.pk as kpi_set_fk
                from static.kpi_set kps;
            """

    @staticmethod
    def get_kpi_data():
        return """
                    select
                            kps.name as kpi_set_name, kps.pk as kpi_set_fk,
                           kpi.display_text as kpi_name, kpi.pk as kpi_fk
                    from static.kpi kpi
                    join static.kpi_set kps on kps.pk = kpi.kpi_set_fk;
                """

    @staticmethod
    def get_atomic_kpi_data():
        return """
                    select
                            kps.name as kpi_set_name, kps.pk as kpi_set_fk,
                            kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                            api.name as atomic_kpi_name, api.pk as atomic_kpi_fk
                    from static.atomic_kpi api
                    join static.kpi kpi on kpi.pk = api.kpi_fk
                    join static.kpi_set kps on kps.pk = kpi.kpi_set_fk;
                    """

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_store_attribute_10(store_fk):
        return """
                select additional_attribute_10 from static.stores
                where pk = {}
                """.format(store_fk)
