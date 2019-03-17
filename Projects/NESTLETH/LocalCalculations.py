
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLETH.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestleth calculations')
    Config.init()
    project_name = 'nestleth'
    data_provider = KEngineDataProvider(project_name)
    # session = 'b8ecbac2-262e-42c6-bb3a-6cdb4a180168' #only one result
    # session = 'F6B7FF4B-1C13-423E-B64B-09F54145E7A0'  multiple results with on shelf
    session = 'b2c89c48-26c0-4a8a-818a-f5b1cf7d62dd'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
