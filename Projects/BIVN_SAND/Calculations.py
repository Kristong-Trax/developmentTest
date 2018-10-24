import os
import datetime as dt

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class BIVN_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()

        if dt.datetime(2018, 7, 1).date() <= self.data_provider.visit_date <= dt.datetime(2018, 8, 31).date():
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'BIVN', 'Data', 'Template_Jul_Aug_2018.xlsx')
        else:
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'BIVN_SAND', 'Data', 'Template.xlsx')

        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('bivn calculations')
#     Config.init()
#     project_name = 'bivn-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'F4499136-47D9-4ADA-B4C4-7114BB32A2AB'
#     data_provider.load_session_data(session)
#     output = Output()
#     BIVN_SANDCalculations(data_provider, output).run_project_calculations()
