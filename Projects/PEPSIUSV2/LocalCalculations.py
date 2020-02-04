
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PEPSIUSV2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('pepsiusv2 calculations')
    Config.init()
    project_name = 'pepsiusv2'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['42E1092B-9D2B-4250-9882-1C4E0A216085', '21014D07-CB94-4545-A2B9-108F3AECD7B8']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
