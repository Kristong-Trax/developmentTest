# -*- coding: utf-8 -*-


class NestleilQueries(object):

    @staticmethod
    def kpi_external_targets_query(operation_type, visit_date):
        query = """SELECT ext.*, ot.operation_type from static.kpi_external_targets ext
                   LEFT JOIN static.kpi_operation_type ot on ext.kpi_operation_type_fk=ot.pk 
                   WHERE
                   ((ext.start_date<='{}' and ext.end_date is null) or 
                   (ext.start_date<='{}' and ext.end_date>='{}'))
                   AND ot.operation_type='{}' order by ext.pk
                """.format(visit_date, visit_date, visit_date, operation_type)
        return query

    @staticmethod
    def get_trax_category_for_products_query():
        query = """SELECT pk as product_fk, trax_category_fk 
                   FROM static_new.product
                """
        return query