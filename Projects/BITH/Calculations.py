
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class BITHCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        if str(self.data_provider.visit_date) >= '2018-02-01' and str(self.data_provider.visit_date) <= '2018-02-28':
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BITH',
                                         'Data', 'Template_old.xlsx')
        else:
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BITH', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofith calculations')
#     Config.init()
#     project_name = 'bith'
#     data_provider = KEngineDataProvider(project_name)
#     session = '09cecbd2-550f-46db-b39a-e52477920aca'
#     data_provider.load_session_data(session)
#     output = Output()
#     BITHCalculations(data_provider, output).run_project_calculations()
