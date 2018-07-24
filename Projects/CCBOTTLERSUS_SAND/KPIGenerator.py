from Trax.Utils.Logging.Logger import Log
from Projects.CCBOTTLERSUS_SAND.Utils.KPIToolBox import log_runtime
from Projects.CCBOTTLERSUS_SAND.REDSCORE.KPIToolBox import CCBOTTLERSUS_SANDREDToolBox


__author__ = 'ortal & ilan & Shivi'


class CCBOTTLERSUS_SANDCcbottlersGenerator:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        self.calculate_red_score()

    @log_runtime('Red Score Calculations')
    def calculate_red_score(self):
        Log.info('starting calculate_red_score')
        for i in xrange(2):
            try:
                tool_box = CCBOTTLERSUS_SANDREDToolBox(self.data_provider, self.output, i)
                tool_box.calculate_red_score()
            except Exception as e:
                Log.error('failed to calculate CCBOTTLERSUS RED SCORE {}: {}'.format(i, e.message))
