from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config
import os
from KPIUtils.GlobalProjects.HEINZ.KPIGenerator import HEINZGenerator
from KPIUtils.DB.Common import Common
import pandas as pd

__author__ = 'Eli'


class HEINZCRCalculations(BaseCalculationsScript):
    """
    https://confluence.trax-cloud.com/pages/resumedraft.action?draftId=174198555&draftShareId=feac8c7a-ec57-4b36-b380-190d3668debc
    """
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        heinz = HEINZGenerator(self.data_provider, self.output, common)
        heinz.heinz_global_distribution_per_category()
        heinz.heinz_global_share_of_shelf_function()
        heinz.heinz_global_price_adherence(pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                    'Config', 'Price Adherence Targets 3Ene2019.xlsx'), sheetname="Price Adherence"))
        heinz.heinz_global_extra_spaces()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('heinzcr-sand calculations')
#     Config.init()
#     project_name = 'heinzcr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     # session = 'efdd2028-6f09-46ff-ad02-18874a6f45b2'
#     sessions = ['7D2A86BC-CC37-42EB-B79D-06A48FD7FEF7']
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         HEINZCRCalculations(data_provider, output).run_project_calculations()
