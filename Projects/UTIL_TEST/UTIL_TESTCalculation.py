from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Logging.Logger import Log
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


class UTIL_TESTCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        # LoggerInitializer.init('UTIL_TEST')
        return Log.info('Running UTIL_TEST calculation')
