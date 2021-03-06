
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

from Projects.DIAGEONORDICS_SAND.Utils.KPIToolBox import DIAGEONORDICS_SANDToolBox, log_runtime

__author__ = 'Elyashiv'


class DIAGEONORDICSGenerator:

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
        self.tool_box = DIAGEONORDICS_SANDToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        log_runtime('Updating templates')(self.tool_box.tools.update_templates)()

        # Todo: Handle Activation Standard ?
        # calculate_activation_standard = False
        # if self.tool_box.ACTIVATION_STANDARD in set_names:
        #     set_names.remove(self.tool_box.ACTIVATION_STANDARD)
        #     calculate_activation_standard = True
        # if calculate_activation_standard:
        #     self.tool_box.calculate_activation_standard()
        set_names = self.tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        self.tool_box.main_calculation(set_names=set_names)
        self.tool_box.commit_results_data()
