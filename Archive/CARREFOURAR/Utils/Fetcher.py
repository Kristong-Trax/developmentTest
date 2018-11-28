import pandas as pd
from datetime import datetime
import numpy

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.CARREFOURAR.Utils.carrefour_tool_box import GENERALToolBox

__author__ = 'ortal'

MAX_PARAMS = 4

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class CARREFOUR_ARKPIFetcher:
    def __init__(self, project_name, scif, matches):
        self.rds_conn = PSProjectConnector(project_name, DbUsers.CalculationEng)
        self.scif = scif
        self.matches = matches
        self.general_tools = GENERALToolBox

    def get_store_number(self):
        query = """
                select st.store_number_1 as store_number
                from static.stores st
                where st.pk = '{}'
                limit 1""".format(self.scif['store_id'][0])

        df = pd.read_sql_query(query, self.rds_conn.db)
        try:
            return df['store_number'][0]
        except IndexError:
            return None

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = (DIAGEOToolBox.DIAGEO, DIAGEOToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = DIAGEOToolBox.DIAGEO
        :return: a filtered Scene Item Facts data frame.
        """
        return self.general_tools.get_filter_condition(df, **filters)

    def calculate_assortment(self, entity, template=None, **filters):
        """
        :param template:
        :param entity:
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique SKUs appeared in the filtered Scene Item Facts data frame.
        """
        assortment = 0
        if template:
            scif = self.scif.loc[self.scif['template_group'] == template]
        else:
            scif = self.scif
        filtered_scif = scif[self.get_filter_condition(scif, **filters)]
        if entity =='SKU':
            assortment = len(filtered_scif['product_ean_code'].unique())
        elif entity == 'brand':
            assortment = len(filtered_scif['brand_fk'].unique())
        return assortment

    def check_survey_answer(self, survey_text, target_answer):
        """
        :param survey_text: The name of the survey in the DB.
        :param target_answer: The required answer/s for the KPI to pass.
        :return: True if the answer matches the target; otherwise - False.
        """
        return self.general_tools.check_survey_answer(survey_text, target_answer)


    @staticmethod
    def get_table_update_query(entries, table, condition):
        updated_values = []
        for key in entries.keys():
            updated_values.append("{} = '{}'".format(key, entries[key][0]))

        query = "UPDATE {} SET {} WHERE {}".format(table, ", ".join(updated_values), condition)

        return query

    def get_atomic_kpi_fk_to_overwrite(self, session_uid, atomic_kpi_fk):
        query = """
                select pk
                from report.kpi_results
                where session_uid = '{}' and atomic_kpi_fk = '{}'
                limit 1""".format(session_uid, atomic_kpi_fk)

        df = pd.read_sql_query(query, self.rds_conn.db)
        try:
            return df['pk'][0]
        except IndexError:
            return None

    def get_kpi_fk_to_overwrite(self, session_uid, kpi_fk):
        query = """
                select pk
                from report.kpk_results
                where session_uid = '{}' and kpi_fk = '{}'
                limit 1""".format(session_uid, kpi_fk)

        df = pd.read_sql_query(query, self.rds_conn.db)
        try:
            return df['pk'][0]
        except IndexError:
            return None

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
    def get_att_4_data():
        return '''
    SELECT p.pk as item_id ,p.att4 as additional_attribute_4
    FROM
    static.product p
    '''

    @staticmethod
    def get_match_display(session_uid):
        return """
            select d.display_name
            from probedata.match_display_in_scene m
            join probedata.scene s on s.pk = m.scene_fk
            join static.display d on d.pk = m.display_fk
            where s.session_uid = '{}'
        """.format(session_uid)

    @staticmethod
    def get_pk_to_delete(session_id):
        return'''select pk  FROM pservice.carrefour_inventory  where session_id = '{}';'''.format(session_id)


    @staticmethod
    def get_question(question_pk):
        return'''SELECT question_text FROM static.survey_question where pk = '{}';'''.format(question_pk)

    @staticmethod
    def get_delete_session_results(session_uid):
        queries = "delete from pservice.carrefour_inventory where pk = '{}';".format(session_uid)
        return queries
