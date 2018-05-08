
__author__ = 'ortalk'


class INTEG15Queries():

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

    # @staticmethod
    # def get_kpi_results_data():
    #     return '''
    #         select api.pk as atomic_kpi_fk
    #         from
    #         static.atomic_kpi api
    #         left join static.kpi kpi on kpi.pk = api.kpi_fk
    #         join static.kpi_set kps on kps.pk = kpi.kpi_set_fk
    #         where kpi.pk = '{}'
    #         limit 1
    #                '''
    #
    #
    # @staticmethod
    # def get_kpk_results_data():
    #     return '''
    #
    #         select k.pk as kpi_fk
    #         from static.kpi k
    #         left join static.kpi_set kp on kp.pk = k.kpi_set_fk
    #         where k.display_text = '{}' and kp.name = '{}'
    #         limit 1
    #                    '''
    #
    #
    # @staticmethod
    # def get_kps_results_data():
    #     return '''
    #         select  kps.name as kps_name, kps.pk
    #         from static.kpi_set kps
    #         where kps.name = '{}'
    #         limit 1
    #                    '''
    #
    # @staticmethod
    # def get_categories():
    #     return '''
    #              SELECT pc.name
    #              FROM static.product_categories pc
    #              where pc.pk={};'''
    #
    # @staticmethod
    # def get_session_facings():
    #     return """
    #              select sv.session_uid, sv.visit_date, p.pk, sv.facings, m.pk as man, c.pk as cat  from
    #              report.scene_visit_summary sv
    #              left join static.product p on p.pk = sv.product_fk
    #              left join static.brand b on b.pk = p.brand_fk
    #              left join static.manufacturers m on m.pk = b.manufacturer_fk
    #              left join static.product_categories c on c.pk =  b.category_fk
    #              where sv.session_uid = '{}' and c.pk = {}
    #                              """

    @staticmethod
    def get_delete_session_results_query(session_uid):
        return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
                "delete from report.kpi_results where session_uid = '{}';".format(session_uid))
