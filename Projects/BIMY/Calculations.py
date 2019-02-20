
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript


import os
# import datetime as dt
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class BIMYCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()

        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                                                       'BIMY', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('bimy calculations')
#     Config.init()
#     project_name = 'bimy'
#     data_provider = KEngineDataProvider(project_name)
#     session = '893de63a-b205-490b-8466-4612708323f5'
#     data_provider.load_session_data(session)
#     output = Output()
#     BIMYCalculations(data_provider, output).run_project_calculations()