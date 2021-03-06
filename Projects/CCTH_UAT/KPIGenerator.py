
from Trax.Utils.Logging.Logger import Log

from Projects.CCTH_UAT.Utils.KPIToolBox import CCTH_UATToolBox, log_runtime
from Projects.CCTH_UAT.SuccessfulVisits import CCTH_UATSuccessfulSessions

__author__ = 'Nimrod'


class CCTH_UATGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = CCTH_UATToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        if self.tool_box.store_type:
            successful = CCTH_UATSuccessfulSessions(self.tool_box.rds_conn, self.session_uid)
            success_status = successful.update_session()
            if success_status == 1:
                Log.info('Session is surveyed - hence calculating KPIs')
                self.tool_box.calculate_red_score()
                self.tool_box.write_gaps_to_db()
            else:
                Log.info('Session is not surveyed - hence skipping KPI calculations')
            self.tool_box.commit_results_data()
        else:
            Log.warning('Store type is empty for this session')

