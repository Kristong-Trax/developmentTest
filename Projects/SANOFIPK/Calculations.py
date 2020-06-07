import os

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config

from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator

__author__ = 'Satya'


class SANOFIPKCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()

        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'SANOFIPK', 'Data', 'Template.xlsx')

        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofipk calculations')
#     Config.init()
#     project_name = 'sanofipk'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'A8D423C9-224E-46F2-8443-E58D96EC9E3D'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIPKCalculations(data_provider, output).run_project_calculations()
