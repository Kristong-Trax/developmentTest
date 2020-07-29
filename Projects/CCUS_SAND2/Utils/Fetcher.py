
__author__ = 'ortalk'


class CCUSQueries:

    def __init__(self):
        pass

    @staticmethod
    def get_all_kpi_data():
        return """
            select api.name as atomic_kpi_name, api.pk as atomic_kpi_fk,
                   kpi.display_text as kpi_name, kpi.pk as kpi_fk,
                   kps.name as kpi_set_name, kps.pk as kpi_set_fk
            from static.kpi_set kps
            left join static.kpi kpi on kpi.kpi_set_fk = kps.pk
            left join static.atomic_kpi api on api.kpi_fk = kpi.pk
        """


    @staticmethod
    def get_atomic_pk_to_delete(session_uid, set_fk):
        return'''SELECT k.pk FROM report.kpi_results k join static.kpi kpi on kpi.pk = k.kpi_fk
        where kpi.kpi_set_fk = '{}' and session_uid = '{}';'''.format(set_fk, session_uid)

    @staticmethod
    def get_kpi_pk_to_delete(session_uid, set_fk):
        return '''SELECT k.pk FROM report.kpk_results k join static.kpi kpi on kpi.pk = k.kpi_fk
            where  kpi.kpi_set_fk = '{}' and session_uid = '{}';'''.format(set_fk, session_uid)

    @staticmethod
    def get_delete_session_results_query(session_uid,set_fk,kpi_pks,atomic_pks):
        if set_fk is not None and atomic_pks is not None and kpi_pks is not None:
            return ('''delete from report.kps_results where session_uid = '{}'
                    and kpi_set_fk = '{}' ;'''.format(session_uid, set_fk),
                    '''delete from report.kpk_results where session_uid = '{}' and pk in {};'''.format(session_uid,
                                                                                                       kpi_pks),
                    '''delete from report.kpi_results where session_uid = '{}' and pk in {} ;'''.format(session_uid,
                                                                                                        atomic_pks))
        # else:
        #     return ("delete from report.kps_results where session_uid = '{}';".format(session_uid),
        #             "delete from report.kpk_results where session_uid = '{}';".format(session_uid),
        #             "delete from report.kpi_results where session_uid = '{}';".format(session_uid))

    @staticmethod
    def get_match_display(session_uid):
        return """
            select sdb.name, m.scene_fk, d.display_name, m.bay_number, m.rect_x, m.rect_y,dt.name as display_type
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            join static.display_brand sdb on sdb.pk=d.display_brand_fk
            join static.display_type dt on dt.pk = d.display_type_fk
            where s.session_uid = '{}'
        """.format(session_uid)


    @staticmethod
    def get_product_atts():
        return """
            SELECT att4,att3,product_ean_code from
            static.product
        """



# import pandas as pd
# from datetime import datetime
# import numpy
#
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
#
#
# __author__ = 'nimrodp'
#
# MAX_PARAMS = 4
#
# KPI_RESULT = 'report.kpi_results'
# KPK_RESULT = 'report.kpk_results'
# KPS_RESULT = 'report.kps_results'
#
#
# class CCUSKPIFetcher:
#     def __init__(self, project_name, scif, matches):
#         self.rds_conn = PSProjectConnector(project_name, DbUsers.CalculationEng)
#         self.scif = scif
#         self.matches = matches
#
#     def get_object_facings(self, scene, params):
#         filtered_results = self.scif[self.scif['scene_id'] == scene]
#         for i in xrange(1, MAX_PARAMS+1):
#             if params.get("Param{}".format(i)):
#                 field = params.get("Param{}".format(i))
#                 values = []
#                 for value in str(params.get("Value{}".format(i))).split(","):
#                     if field == "size":
#                         value = float(value)
#                         if int(value) == value:
#                             value = int(value)
#                     elif 'fk' in field or 'number' in field:
#                         value = int(float(value))
#                     values.append(value)
#                 filtered_results = filtered_results.loc[filtered_results[field].isin(values)]
#             else:
#                 break
#         return filtered_results['facings'].to_dict()
#
#     @staticmethod
#     def get_table_update_query(entries, table, condition):
#         updated_values = []
#         for key in entries.keys():
#             updated_values.append("{} = '{}'".format(key, entries[key][0]))
#
#         query = "UPDATE {} SET {} WHERE {}".format(table, ", ".join(updated_values), condition)
#
#         return query
#
#     def get_atomic_kpi_fk_to_overwrite(self, session_uid, atomic_kpi_fk):
#         query = """
#                 select pk
#                 from report.kpi_results
#                 where session_uid = '{}' and atomic_kpi_fk = '{}'
#                 limit 1""".format(session_uid, atomic_kpi_fk)
#
#         df = pd.read_sql_query(query, self.rds_conn.db)
#         try:
#             return df['pk'][0]
#         except IndexError:
#             return None
#
#     def get_kpi_fk_to_overwrite(self, session_uid, kpi_fk):
#         query = """
#                 select pk
#                 from report.kpk_results
#                 where session_uid = '{}' and kpi_fk = '{}'
#                 limit 1""".format(session_uid, kpi_fk)
#
#         df = pd.read_sql_query(query, self.rds_conn.db)
#         try:
#             return df['pk'][0]
#         except IndexError:
#             return None
