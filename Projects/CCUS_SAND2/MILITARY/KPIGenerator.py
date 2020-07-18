from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.CCUS_SAND2.MILITARY.Utils.KPIToolBox import MilitaryToolBox


class MilitaryGenerator:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.tool_box = MilitaryToolBox(data_provider, output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        self.tool_box.main_calculation()
        self.tool_box.commit_results()
