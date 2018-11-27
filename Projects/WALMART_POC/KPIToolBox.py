from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup, SessionInfo
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector


class KPIToolBox:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.scenes_info = self.data_provider[Data.SCENES_INFO]
        self.rds_conn = PSProjectConnector(self.data_provider.project_name, DbUsers.CalculationEng)
