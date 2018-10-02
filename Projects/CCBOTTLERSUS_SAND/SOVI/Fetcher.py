__author__ = 'huntery'


class SQLQueries(object):

    @staticmethod
    def get_kpi_level_2_results_max_pk():
        return """SELECT MAX(pk) FROM report.kpi_level_2_results"""
