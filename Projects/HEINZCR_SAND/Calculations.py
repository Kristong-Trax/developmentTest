from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from KPIUtils.GlobalProjects.HEINZ.KPIGenerator import HEINZGenerator
# from Projects.HEINZCR_SAND.KPIGenerator import HEINZCRGenerator
from KPIUtils.DB.Common import Common
import pandas as pd

__author__ = 'Eli'


class HEINZCRCalculations(BaseCalculationsScript):
    """
    https://confluence.trax-cloud.com/pages/resumedraft.action?draftId=174198555&draftShareId=feac8c7a-ec57-4b36-b380-190d3668debc
    """
    def run_project_calculations(self):
        self.timer.start()
        # HEINZCRGenerator(self.data_provider, self.output).main_function()
        common = Common(self.data_provider)
        heinz = HEINZGenerator(self.data_provider, self.output, common)
        heinz.heinz_global_distribution_per_category()
        heinz.heinz_global_share_of_shelf_function()
        heinz.heinz_global_price_adherence(pd.read_excel("./Config/price_List.xlsx", sheetname="Price Adherence"))
        heinz.heinz_global_extra_spaces()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('heinzcr-sand calculations')
#     Config.init()
#     project_name = 'heinzcr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'efdd2028-6f09-46ff-ad02-18874a6f45b2'
#     # sessions = ['7D2A86BC-CC37-42EB-B79D-06A48FD7FEF7', '040c48e9-bfb7-48e7-9970-105a9f78182b','7b60f2d4-b8cf-4f82-94da-6e5579cf0e4b', '6d17d4d2-9af6-4d91-8149-bbe0cd674bc9', '79734d6e-9d2c-4299-8a1f-ba810aa2434c', 'f6cc1ed0-978f-4b82-bf9a-0112dee9b974', '826cf809-b94e-4649-90ee-934142c5ba92', '96ee6087-7df4-4243-8dde-5e0cdcaaee26','72026634-ca71-41b6-8bef-ef317f15d082']
#     # for session in sessions:
#     data_provider.load_session_data(session)
#     output = Output()
#     HEINZCRCalculations(data_provider, output).run_project_calculations()
