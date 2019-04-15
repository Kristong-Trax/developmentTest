import os
import datetime as dt

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config

from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class BIVNCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()

        if dt.datetime(2018, 7, 1).date() <= self.data_provider.visit_date <= dt.datetime(2018, 8, 31).date():
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'BIVN', 'Data', 'Template_Jul_Aug_2018.xlsx')
        else:
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'BIVN', 'Data', 'Template.xlsx')

        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('sanofivn calculations')
#     Config.init()
#     project_name = 'bivn'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'B3988C35-769C-4EAE-8E25-5250AD7E4CBD'
#     data_provider.load_session_data(session)
#     output = Output()
#     BIVNCalculations(data_provider, output).run_project_calculations()
