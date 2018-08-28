
__author__ = 'natalyak'

class CCBZA_Queries(object):
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
    def get_store_data_by_store_id(store_fk):
        return """
                select s.store_type, s.additional_attribute_1, s.additional_attribute_2
                from static.stores s
                where s.pk = {}
            """.format(store_fk)

    @staticmethod
    def get_new_kpi_data():
        return """
                select *
                from static.kpi_level_2
            """

    @staticmethod
    def get_planogram_results(scenes):
        return """
               SELECT * 
               FROM report.match_planogram_compliance
               where scene_fk in {}
               """.format(scenes)

    @staticmethod
    def get_kpi_result_values():
        return """
                SELECT *
                FROM static.kpi_result_value
                """

    @staticmethod
    def get_kpi_score_values():
        return """
                    SELECT *
                    FROM static.kpi_score_value
                    """
    @staticmethod
    def get_manufacturer_pk_by_name(manufacturer_name):
        return """
                    SELECT pk
                    FROM static_new.manufacturer 
                    WHERE name = '{}'
                    """.format(manufacturer_name)
