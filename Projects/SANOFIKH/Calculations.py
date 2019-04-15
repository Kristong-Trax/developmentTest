import os
import datetime as dt

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFIKHCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()

        if dt.datetime(2018, 7, 1).date() <= self.data_provider.visit_date <= dt.datetime(2018, 8, 31).date():
            template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'SANOFIKH', 'Data', 'Template_Jul_Aug_2018.xlsx')
        else:
            template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'SANOFIKH', 'Data', 'Template.xlsx')

        SANOFIGenerator(self.data_provider, self.output, template_path).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('sanofikh calculations')
    Config.init()
    project_name = 'sanofikh'
    data_provider = KEngineDataProvider(project_name)
    session = '9733c7d4-20c7-4d57-859e-c918b6209d04'
    data_provider.load_session_data(session)
    output = Output()
    SANOFIKHCalculations(data_provider, output).run_project_calculations()
