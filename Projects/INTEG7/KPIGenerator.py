
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log

from Projects.INTEG7.Utils.KPIToolBox import INTEG7ToolBox, log_runtime

__author__ = 'Nimrod'


class INTEG7Generator:

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.output = output
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.tool_box = INTEG7ToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        set_names = self.tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        for kpi_set_name in set_names:
            self.tool_box.main_calculation(set_name=kpi_set_name)
        Log.info('Downloading templates took {}'.format(self.tool_box.download_time))
        self.tool_box.commit_results_data()
