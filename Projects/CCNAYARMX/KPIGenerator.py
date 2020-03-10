from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.CCNAYARMX.Utils.KPIToolBox import ToolBox
from Projects.CCNAYARMX.National.Utils.KPIToolBox import NationalToolBox
from Projects.CCNAYARMX.Especializado.Utils.KPIToolBox import EspecializadoToolBox
from Projects.CCNAYARMX.Fondas.Utils.KPIToolBox import FONDASToolBox

from KPIUtils_v2.DB.CommonV2 import Common
__author__ = 'krishnat'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        # self.tool_box = ToolBox(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        # if self.tool_box.scif.empty:
        #     Log.warning('Scene item facts is empty for this session')
        # self.tool_box.main_calculation()
        # self.tool_box.commit_results()
        common = Common(self.data_provider)
        especializado_tool_box = EspecializadoToolBox(self.data_provider,self.output, common)
        especializado_tool_box.main_calculation()

        fondas_tool_box = FONDASToolBox(self.data_provider, self.output, common)
        fondas_tool_box.main_calculation()
        # fondas_tool_box.commit_results()

        tool_box = ToolBox(self.data_provider, self.output, common)
        tool_box.main_calculation()

        nayar_tool_box = NationalToolBox(self.data_provider, self.output, common)
        nayar_tool_box.main_calculation()
        nayar_tool_box.commit_results()

    # @log_runtime('Original Nayar Calculations')
    # def caculate_original_nayar(self):
    #     tool_box = ToolBox(self.data_provider, self.output)
    #     tool_box.main_calculation()

    # @log_runtime('National Nayar Calculations')
    # def calculate_national_nayar(self):
    #     tool_box = NationalToolBox(self.data_provider, self.output)
    #     tool_box.main_calculation()
    #     tool_box.commit_results()
