
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.GSKNZ.Calculations import Calculations


#  if __name__ == '__main__':
#     LoggerInitializer.init('gsknz calculations')
#     Config.init()
#     project_name = 'gsknz'
#     data_provider = KEngineDataProvider(project_name)
#     # session = 'E2085763-A571-46A9-8D6F-12BD6E993C2B' # 'FACDB87C-AFA8-4F8E-BAE5-0CAFBADC702C'
#
#     session = 'DA26825C-457D-4378-96A4-5154E87B2380'
#     # session = 'F8AA57DE-E69E-4811-8C7D-8E2629EC2969'
#
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()


