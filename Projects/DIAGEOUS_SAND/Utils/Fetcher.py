
__author__ = 'Elyashiv'


class DIAGEOUSQueries(object):

    @staticmethod
    def get_result_values():
        return "SELECT * FROM static.kpi_result_value;"

    @staticmethod
    def get_state():
        return "select name from static.state where pk = {};"

    @staticmethod
    def get_sub_brands():
        return "SELECT * FROM static.custom_entity where entity_type_fk = 1002;"

    @staticmethod
    def insert_new_sub_brands():
        return """INSERT INTO static.custom_entity (name, entity_type_fk, parent_id)
                VALUES ("{}", "1002", "2")"""

    @staticmethod
    def get_prices_dataframe():
        return """SELECT p.scene_fk as scene_fk, mpip.product_fk as product_fk,
                    mpn.match_product_in_probe_fk as probe_match_fk,
                    mpn.value as price_value, mpas.state as number_attribute_state
            FROM
                    probedata.match_product_in_probe_price_attribute_value mpn
                    LEFT JOIN static.match_product_in_probe_attributes_state mpas ON mpn.attribute_state_fk = mpas.pk
                    JOIN probedata.match_product_in_probe mpip ON mpn.match_product_in_probe_fk = mpip.pk
                    JOIN probedata.probe p ON p.pk = mpip.probe_fk
            WHERE
                    p.session_uid = "{}";"""
