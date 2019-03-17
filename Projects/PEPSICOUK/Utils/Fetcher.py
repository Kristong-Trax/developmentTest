
__author__ = 'natalyak'

class PEPSICOUK_Queries(object):
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

    @staticmethod
    def on_display_products_query(probe_match_list):
        if len(probe_match_list) == 1:
            query = """
                    SELECT statevalue.match_product_in_probe_fk as probe_match_fk, 
                    sstate.name as smart_attribute
                    FROM probedata.match_product_in_probe_state_value statevalue
                    join static.match_product_in_probe_state sstate 
                    on statevalue.match_product_in_probe_state_fk=sstate.pk
                    where sstate.name = 'additional display'
                    and statevalue.match_product_in_probe_fk in ({});
                    """.format(probe_match_list[0])
        else:
            query = """
                    SELECT statevalue.match_product_in_probe_fk as probe_match_fk, 
                    sstate.name as smart_attribute
                    FROM probedata.match_product_in_probe_state_value statevalue
                    join static.match_product_in_probe_state sstate 
                    on statevalue.match_product_in_probe_state_fk=sstate.pk
                    where sstate.name = 'additional display'
                    and statevalue.match_product_in_probe_fk in {};
                    """.format(tuple(probe_match_list))
        return query

    @staticmethod
    def get_custom_entities_query():
        query = """SELECT * from static.custom_entity"""
        return query

    @staticmethod
    def get_kpi_external_targets(visit_date):
        return """SELECT * from static.kpi_external_targets 
                  where (start_date<='{}' and end_date is null) or 
                  (start_date<='{}' and end_date>='{}')""".format(visit_date, visit_date, visit_date)