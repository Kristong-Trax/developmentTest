
from Trax.Utils.Logging.Logger import Log
from Projects.RINIELSENUS.Utils.KPIToolBox import MarsUsDogMainMealWet
from Projects.RINIELSENUS.Utils.Utils import log_runtime
from Projects.RINIELSENUS.PURINA.KPIToolBox import PURINAToolBox
from Projects.RINIELSENUS.MILLERCOORS.Utils.KPIToolBox import MILLERCOORSToolBox


__author__ = 'nethanel'


class MarsUsGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = MarsUsDogMainMealWet(self.data_provider, self.output)
        # self.purina_tool_box = PURINAToolBox(self.data_provider, self.output)
        # self.millercoors_tool_box = MILLERCOORSToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.data_provider.scene_item_facts.empty:
            Log.warning('Scene item facts is empty for this session')
        try:
            self.tool_box.calculate_scores()
        except:
            Log.error('Mars US kpis not calculated')

        # try:
        #     self.purina_tool_box.calculate_purina()
        # except:
        #     Log.error('Purina kpis not calculated')

        # try:
        #     self.millercoors_tool_box.main_calculation()
        #     self.millercoors_tool_box.commit_results()
        # except:
        #     Log.error('MillerCoors KPIs not calculated')
