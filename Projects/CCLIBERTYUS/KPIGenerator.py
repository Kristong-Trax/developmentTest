
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.CCLIBERTYUS.SOVI.KPIToolBox import SOVIToolBox
from Projects.CCLIBERTYUS.MSC.KPIToolBox import MSCToolBox
from Projects.CCLIBERTYUS.LIBERTY.KPIToolBox import LIBERTYToolBox

from KPIUtils_v2.DB.CommonV2 import Common as CommonV2

__author__ = 'Hunter'


class CCLIBERTYUSGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common_v2 = CommonV2(self.data_provider)

    @log_runtime('Total CCLIBERTYUSCalculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        self.calculate_sovi()
        self.calculate_msc()
        self.calculate_liberty()
        self.common_v2.commit_results_data()  # saves results to new tables

    @log_runtime('SOVI CCLIBERTYUSCalculations')
    def calculate_sovi(self):
        Log.info('starting calculate_sovi')
        try:
            tool_box = SOVIToolBox(self.data_provider, self.output, self.common_v2)
            tool_box.main_calculation()
        except Exception as e:
            Log.error('failed to calculate SOVI due to :{}'.format(e.message))

    @log_runtime('MSC CCBOTTERSUSCalculations')
    def calculate_msc(self):
        Log.info('starting calculate_msc')
        try:
            tool_box = MSCToolBox(self.data_provider, self.output, self.common_v2)
            tool_box.main_calculation()
        except Exception as e:
            Log.error('failed to calculate MSC Compliance due to: {}'.format(e.message))

    @log_runtime('LIBERTY CCBOTTERSUSCalculations')
    def calculate_liberty(self):
        Log.info('starting calculate_liberty')
        try:
            tool_box = LIBERTYToolBox(self.data_provider, self.output, self.common_v2)
            tool_box.main_calculation()
        except Exception as e:
            Log.error('failed to calculate LIBERTY KPIs due to: {}'.format(e.message))
