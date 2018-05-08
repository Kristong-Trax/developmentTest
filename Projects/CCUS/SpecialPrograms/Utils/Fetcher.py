
__author__ = 'Shani'


class SpecialProgramsQueries(object):

    # @staticmethod
    # def get_all_kpi_data():
    #     return """
    #         select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
    #                kpi.display_text as kpi_name, kpi.pk as kpi_fk,
    #                kps.name as kpi_set_name, kps.pk as kpi_set_fk
    #         from static.atomic_kpi api
    #         left join static.kpi kpi on kpi.pk = api.kpi_fk
    #         join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
    #     """

    @staticmethod
    def get_all_kpi_data():
        return """
            select
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from
            static.kpi kpi
            join static.kpi_set kps on kps.pk = kpi.kpi_set_fk"""

    @staticmethod
    def get_atomic_pk_to_delete(session_uid, set_fk):
        return '''SELECT k.pk FROM report.kpi_results k join static.kpi kpi on kpi.pk = k.kpi_fk
           where kpi.kpi_set_fk = '{}' and session_uid = '{}';'''.format(set_fk, session_uid)

    @staticmethod
    def get_delete_session_results_query(session_uid, set_fk, atomic_pks):
        if set_fk is not None and atomic_pks is not None:
            return ('''delete from report.kps_results where session_uid = '{}'
                        and kpi_set_fk = '{}' ;'''.format(session_uid, set_fk),
                    '''delete from report.kpi_results where session_uid = '{}' and pk in ({}) ;'''.format(session_uid,
                                                                                                          ','.join(
                                                                                                              [str(
                                                                                                                  atomic_pk)
                                                                                                               for
                                                                                                               atomic_pk
                                                                                                               in
                                                                                                               atomic_pks]
                                                                                                          )))
    @staticmethod
    def get_product_atts():
        return """
            SELECT att3, att4, product_ean_code from
            static.product
        """
