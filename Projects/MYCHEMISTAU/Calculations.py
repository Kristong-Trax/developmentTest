from Projects.MYCHEMISTAU.MyChemist import MyChemistReport

# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Logging.Logger import Log

PROJECT = 'mychemistau'
__author__ = 'tall'

class MYCHEMISTAUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Log.info('Starting project calculations for mychemistau')
        mychemist_report = MyChemistReport(self.data_provider.project_name)
        mychemist_report.connect()
        mychemist_report.generate_report(session_uid=self.data_provider.session_uid)
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('{} calculations'.format(PROJECT))
#     Config.init()
#     project_name = PROJECT
#     data_provider = KEngineDataProvider(project_name)
#     session = '4d2ac98c-cb10-4212-8bc3-5f3f6eaf33a7'
#     data_provider.load_session_data(session)
#     output = Output()
#     MYCHEMISTAUCalculations(data_provider, output).run_project_calculations()
