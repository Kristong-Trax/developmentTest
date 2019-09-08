from KPIUtils_v2.Utils.Consts.GlobalConsts import BasicConsts
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Projects.TNUVAILV2.Data.LocalConsts import Consts
from pandas.io.sql import DatabaseError
from Trax.Utils.Logging.Logger import Log
import pandas as pd


class DBHandler:
    """
    Tnuva has NCC report that comparing the results of the OOS SKU level for the current session and the previous
    ones. We didn't want to calculate it during the report and this doesn't exist yet in the API so this util class
    is handling on fetching the results.
    """
    def __init__(self, project_name, session_uid):
        self.project_name = project_name
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_uid = session_uid

    def _get_previous_session_fk(self):
        """
        This method fetches the last completed session_fk for the current store.
        """
        last_session_fk_query = self._get_last_visit_fk_query()
        last_session_fk = self._execute_db_query(last_session_fk_query)
        if len(last_session_fk) != 2:
            Log.warning(Consts.LOG_EMPTY_PREVIOUS_SESSIONS.format(self.session_uid))
            last_session_fk = None
        else:
            last_session_fk = last_session_fk.loc[1, BasicConsts.PK]
        return last_session_fk

    def _get_oos_results(self, session_fk):
        """
        This method gets a session_fk and fetches the relevant OOS results.
        """
        query = self._previous_oos_results_query(session_fk)
        result = self._execute_db_query(query)
        return result

    def get_last_session_oos_results(self):
        """
        This is the main method of this util and the only public one.
        It fetches the relevant OOS results for the last relevant visit if exists.
        """
        last_session_fk = self._get_previous_session_fk()
        if last_session_fk is None:
            return None
        oos_results = self._get_oos_results(last_session_fk)
        return oos_results

    def get_kpi_result_value(self):
        """ This method extracts the kpi_result_types from the DB. """
        result_type_query = self._get_kpi_result_value_query()
        result_types = self._execute_db_query(result_type_query)
        return result_types

    def _execute_db_query(self, query):
        """ This method is responsible on the DB execution.
        It gets a query (string) and executes it. """
        try:
            result = pd.read_sql_query(query, self.rds_conn.db)
        except DatabaseError:
            self.rds_conn.connect_rds()
            result = pd.read_sql_query(query, self.rds_conn.db)
        return result

    # The following are the queries that we are using in order to get the previous
    # sessions relevant results.

    @staticmethod
    def _previous_oos_results_query(session_fk):
        """
        This m
        :param session_fk:
        :return:
        """
        prev_results_query = """SELECT 
                                kpi_level_2_fk, numerator_id, result
                            FROM
                                report.kpi_level_2_results
                            WHERE
                                session_fk = {}
                                    AND kpi_level_2_fk IN (SELECT 
                                        pk
                                    FROM
                                        static.kpi_level_2
                                    WHERE TYPE IN {})
                                       """.format(session_fk, Consts.PREV_RES_KPIS_FOR_NCC)
        return prev_results_query

    @staticmethod
    def _get_kpi_result_value_query():
        kpi_result_type = """SELECT pk, value FROM static.kpi_result_value;"""
        return kpi_result_type

    def _get_last_visit_fk_query(self):
        """
        Before fetching the results we need to get the previous session from the same store.
        This is a query that used by the Mobile team.
        This query returns the current session's fk and the previous one as well.
        """
        last_two_sessions_query = """SELECT
                   s1.pk
                FROM
                   probedata.session s1
                JOIN probedata.session s2 ON
                   s2.store_fk = s1.store_fk
                   AND s2.visit_date >= s1.visit_date
                   AND s2.start_time >= s1.start_time
                   AND (
                       SELECT count(1)
                   from
                       probedata.scene as sc
                   where
                       sc.session_uid = s2.session_uid
                       and status <> 6
                       AND sc.delete_time is null) = 0
                WHERE
                   s2.session_uid = '{}'
                   AND s2.delete_time is NULL
                   AND s1.delete_time is NULL
                   AND (
                       SELECT count(1)
                   from
                       probedata.scene as sc
                   where
                       sc.session_uid = s1.session_uid
                       and status <> 6
                       AND sc.delete_time is null) = 0
                ORDER BY
                   s1.visit_date DESC ,
                   s1.start_time DESC
                limit 2;""".format(self.session_uid)
        return last_two_sessions_query