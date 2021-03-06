from Trax.Utils.Logging.Logger import Log

from Projects.CCBZA_SAND.Utils.KPIToolBox import CCBZA_SANDToolBox

from KPIUtils_v2.DB.Common import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'natalyak'


class CCBZA_SANDGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = CCBZA_SANDToolBox(self.data_provider, self.output)
        # self.common = Common(data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        # for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():
        #     score = self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
        #     self.common.write_to_db_result(kpi_set_fk, self.tool_box.LEVEL1, score)
        # self.common.commit_results_data()

        # Alternative
        # if self.tool_box.template_data:
        #     self.tool_box.main_calculation()
        # else:
        #     Log.warning('No template data is available')

    @log_runtime('Total Scene Calculations', log_start=True)
    def main_scene_function(self):
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.scene_main_calculation()
