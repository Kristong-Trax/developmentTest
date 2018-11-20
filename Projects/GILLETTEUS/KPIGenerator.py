
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

from Projects.GILLETTEUS.Utils.KPIToolBox import GILLETTEUSToolBox, log_runtime

__author__ = 'Nimrod'


class GILLETTEUSGenerator:

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.output = output
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.tool_box = GILLETTEUSToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.store_type == self.tool_box.STORE_TYPE:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            self.tool_box.main_calculations()
            self.tool_box.commit_results_data()
        else:
            Log.info('KPIs are not calculated for store_type {}'.format(self.tool_box.store_type))
