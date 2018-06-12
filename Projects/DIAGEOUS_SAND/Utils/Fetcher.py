
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
