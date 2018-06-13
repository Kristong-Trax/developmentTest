
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand calculations')
    Config.init()
    project_name = 'diageous'
    sessions = [
        "A7F38201-6471-46ED-A5E1-56BB577F1CA8",
        "649B571C-948A-4F78-AA85-277822D4CC8C",
        "91810AA5-A960-4F75-A00C-F6125A0E6935",
        "CFF663F6-D2C8-453C-B901-95BE44368809",
        "23BCAB94-61D0-4C61-A86F-C939ED891FEC",
        "AAF26089-C999-456F-9858-09C24C09112B",
        "D1633840-68A9-4715-BBAC-DDDD130FA337",
        "C971C0DC-B14B-4E94-9987-D0F37A2E6CE2",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
