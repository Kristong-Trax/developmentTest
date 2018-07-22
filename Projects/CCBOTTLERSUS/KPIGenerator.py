# from Trax.Algo.Calculations.Core.DataProvider import Data
# from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
#
#
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils.DB.Common import Common
from Projects.CCBOTTLERSUS.DISPLAYS.KPIToolBox import DISPLAYSToolBox
from Projects.CCBOTTLERSUS.Utils.KPIToolBox import log_runtime, BCIKPIToolBox
from Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox import REDToolBox


__author__ = 'ortal & ilan & Shivi'


class CcbottlersGenerator:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        # Common(self.data_provider).commit_results_data()
        # self.calculate_bci()
        # self.calculate_manufacturer_displays()
        self.calculate_red_score()

    @log_runtime('Manufacturer Displays Calculations')
    def calculate_manufacturer_displays(self):
        tool_box = DISPLAYSToolBox(self.data_provider, self.output)
        set_names = tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        for kpi_set_name in set_names:
            if kpi_set_name == 'Manufacturer Displays':
                tool_box.main_calculation()
                set_fk = tool_box.kpi_static_data[tool_box.kpi_static_data['kpi_set_name'] == kpi_set_name][
                    'kpi_set_fk'].values[0]
                tool_box.commit_results_data(kpi_set_fk=set_fk)

    @log_runtime('Bci Calculations')
    def calculate_bci(self):
        tool_box = BCIKPIToolBox(self.data_provider, self.output)
        if tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        set_names = tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        tool_box.tools.update_templates()
        for kpi_set_name in set_names:
            if kpi_set_name == 'BCI':
                tool_box.main_calculation(set_name=kpi_set_name)
                tool_box.save_level1(set_name=kpi_set_name, score=100)
                Log.info('calculate kpi took {}'.format(tool_box.download_time))
                set_fk = tool_box.kpi_static_data[tool_box.kpi_static_data['kpi_set_name'] == kpi_set_name]['kpi_set_fk'].values[0]
                tool_box.commit_results_data(kpi_set_fk=set_fk)

    @log_runtime('Red Score Calculations')
    def calculate_red_score(self):
        Log.info('starting calculate_red_score')
        for i in xrange(2):
            try:
                tool_box = REDToolBox(self.data_provider, self.output, i)
                tool_box.calculate_red_score()
            except Exception as e:
                Log.error('failed to calculate CCBOTTLERSUS RED SCORE {}: {}'.format(i, e.message))
        # tool_box = REDToolBox(self.data_provider, self.output, 1)
        # tool_box.calculate_red_score()
