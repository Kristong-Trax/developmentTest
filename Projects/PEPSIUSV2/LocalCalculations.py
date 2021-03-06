
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PEPSIUSV2.Calculations import Calculations
from Trax.Utils.Conf.Configuration import Config


if __name__ == '__main__':
    LoggerInitializer.init('pepsiusv2 calculations')
    Config.init()
    project_name = 'pepsiusv2'
    data_provider = KEngineDataProvider(project_name)
    session_list = [
        '1f6e3234-0f43-405f-88b4-9e10cc434351',
        '00fa1a34-8471-46a6-bcb4-d7e9186844b1',
        '35e2c350-7652-4843-a3cd-481220fbd418',
        '6a07a5bf-7e09-4cbb-855f-4a68c6ffab93',
        '7f799dad-3a94-4ff0-9f6c-59dd3ed09218'
    ]
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
