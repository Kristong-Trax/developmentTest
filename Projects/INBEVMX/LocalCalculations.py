
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('inbevmx calculations')
    Config.init()
    project_name = 'inbevmx'
    data_provider = KEngineDataProvider(project_name)
    list_sessions = [

                'e840f5e3-ca0f-4e1a-a47b-13c655cdce47',
                '0707768f-05d0-4872-b5af-706f45a5d8a0',
                '86c209de-c40e-4e8b-a2fe-a2b7bf81b89b'
                # 'fff20792-6a60-4a13-bb00-879a308c1ea6',
                # '341a9b53-65ad-43b6-9fe0-2ae56fcbe9bd',
                # '6a19080f-9741-4760-85fb-22a5e774d13b'
            ]
    output = Output()
    for session in list_sessions:
        data_provider.load_session_data(session)
        Calculations(data_provider, output).run_project_calculations()
