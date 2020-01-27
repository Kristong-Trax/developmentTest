import os

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from KPIUtils.GlobalProjects.SANOFI_3.KPIGenerator import SANOFIGenerator

__author__ = 'Satya'


class SANOFIPKCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()

        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'SANOFIPK', 'Data', 'Template.xlsx')

        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('sanofipk calculations')
    Config.init()
    project_name = 'sanofipk'
    data_provider = KEngineDataProvider(project_name)
    session = 'B3988C35-769C-4EAE-8E25-5250AD7E4CBD'
    data_provider.load_session_data(session)
    output = Output()
    SANOFIPKCalculations(data_provider, output).run_project_calculations()
