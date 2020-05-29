__author__ = 'nicolaskeeton'


class DiageoQueries(object):

    @staticmethod
    def get_custom_entities_query():
        return """SELECT * from static.custom_entity"""
