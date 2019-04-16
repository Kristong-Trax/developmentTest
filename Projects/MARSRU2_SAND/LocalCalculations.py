from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU2_SAND.Calculations import MARSRU2_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('MARSRU calculations')
    Config.init()
    project_name = 'marsru2-sand'
    session_uids = ['1A70C0D6-E756-45FB-87FE-9EEFD8F64723',
                    # 'fff1519b-5aea-4426-a3bc-18d30a2d789d',
                    # 'f9eea954-dd2b-4fbd-a1b3-36301681e15c',
                    # 'f960b439-a788-4f14-af2d-f3835483d3d3',
                    # 'f0bf3cc1-c0bc-42b8-b169-19d2c28e9232'
                    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU2_SANDCalculations(data_provider, output).run_project_calculations()
