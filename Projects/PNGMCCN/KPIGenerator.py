
import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log

from Projects.PNGMCCN.Utils.KPIToolBox import PNGMCCNToolBox

__author__ = 'Nimrod'


class PNGMCCNGenerator:

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
        self.tool_box = PNGMCCNToolBox(self.data_provider, self.output)

    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        calc_start_time = datetime.datetime.utcnow()
        Log.info('Calculation Started at {}'.format(calc_start_time))
        if not self.tool_box.scif.empty:
            self.tool_box.main_calculation()
            # Saving a dummy result into Level1 (KPI set)
            for set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique():
                category = self.tool_box.kpi_static_data[self.tool_box.kpi_static_data['kpi_set_fk'] ==
                                                         set_fk]['kpi_set_name'].values[0]
                if self.tool_box.validate_category(category):
                    self.tool_box.write_to_db_result(fk=set_fk, score=100, level=self.tool_box.LEVEL1)
        else:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.commit_results_data()
        calc_finish_time = datetime.datetime.utcnow()
        Log.info('Calculation time took {}'.format(calc_finish_time - calc_start_time))
