from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from pandas.io.sql import DatabaseError
from Projects.TNUVAILV2.Utils.Consts import Consts


class PrevResHandler:
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
        :return:
        """
        last_session_fk_query = self._get_last_visit_fk_query()
        last_session_fk = self._execute_db_query(last_session_fk_query)
        if len(last_session_fk) != 2:
            Log.warning(Consts.LOG_EMPTY_PREVIOUS_SESSIONS)
            last_session_fk = None
        else:
            last_session_fk = last_session_fk.loc[1, 'pk']
        return last_session_fk

    def _get_oos_results(self, session_fk):
        """

        :param session_fk:
        :return:
        """
        query = self._previous_oos_results_query(session_fk)
        result = self._execute_db_query(query)
        return result

    def _get_last_session_oos_results(self):
        """
        :return:
        """
        last_session_fk = self._get_previous_session_fk()
        if last_session_fk is None:
            return
        oos_results = self._get_oos_results(last_session_fk)
        return oos_results

    def _execute_db_query(self, query):
        """
        """
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
                                session_fk = 1428887
                                    AND kpi_level_2_fk IN (SELECT 
                                        pk
                                    FROM
                                        static.kpi_level_2
                                    WHERE
                                        kpi_calculation_stage_fk = '3'
                                            AND type LIKE '%OOS%'
                                            AND type LIKE '%SKU%');""".format(session_fk, Consts.PS_CALC_STAGE)
        return prev_results_query

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
