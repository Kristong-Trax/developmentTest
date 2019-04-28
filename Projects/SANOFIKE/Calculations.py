
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
import os
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFIKECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIKE', 'Data', 'Template.xlsx')
        TEMPLATE_PATH2 = TEMPLATE_PATH.replace("/Template.xlsx", "/template_jan.xlsx")
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer as Log
if __name__ == '__main__':
    Log.init('sanofike calculations')
    Config.init()
    project_name = 'sanofike'
    data_provider = KEngineDataProvider(project_name)
    session = '9CF0B856-0D61-4456-8F2A-AC588457FD13'
    data_provider.load_session_data(session)
    output = Output()
    SANOFIRUCalculations(data_provider, output).run_project_calculations()
