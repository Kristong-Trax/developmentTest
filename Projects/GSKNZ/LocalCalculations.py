
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.GSKNZ.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gsknz calculations')
    Config.init()
    project_name = 'gsknz'
    data_provider = KEngineDataProvider(project_name)
    session = 'E2085763-A571-46A9-8D6F-12BD6E993C2B' # FACDB87C-AFA8-4F8E-BAE5-0CAFBADC702C

    # session = 'fdae28d2-6e49-45d2-b357-cd9ca7a13de6'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()


