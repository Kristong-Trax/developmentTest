from KPIUtils_v2.DB.CommonV2 import Common
import sys
import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Trax.Algo.Calculations.Core.KPI.OutputWriter import KpiResultsOutputWriter
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

class NotCommon(Common):
    @log_runtime('Saving to DB')
    def commit_results_data(self, result_entity=Common.SESSION, scene_session_hierarchy=False, delete_results=True,
                            targets=None):
        """
        HACK FIX because rinielsenus doesn't use common to commit results, but common is used to write to
        report.match_product_in_probe_state_value_reporting and common only saves those if there are other
        kpi results to save.

        God rest the soul of he who inherits....
        """
        if delete_results:
            delete_queries = {'delete_old_session_specific_tree_query': ''}
            if not self.match_product_in_probe_state_values.empty:
                match_product_in_probe_fks = self.match_product_in_probe_state_values[self.MATCH_PRODUCT_IN_PROBE_FK].values
                delete_queries['delete_costume_smart_att'] = \
                    self.queries.get_delete_match_product_in_probe_state_value_reporting_results_query(match_product_in_probe_fks).replace(",)", ")")

            local_con = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
            cur = local_con.db.cursor()
            for key, value in delete_queries.iteritems():
                if key == 'delete_old_session_specific_tree_query' and not value:
                    continue
                cur.execute(value)
        else:
            local_con = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
            cur = local_con.db.cursor()
        Log.debug('Write Trax Explorer Filter Values')
        if not self.match_product_in_probe_state_values.empty:
            costume_smart_att_query = self.get_insert_match_product_in_probe_state_data_to_db()
            cur.execute(costume_smart_att_query)
        local_con.db.commit()
        return

    def read_custom_query(self, query):
        local_con = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        df = pd.read_sql_query(query, local_con.db)
        local_con.disconnect_rds()
        return df