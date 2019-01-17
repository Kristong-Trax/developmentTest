
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.TWEAU.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('tweau calculations')
    Config.init()
    project_name = 'tweau'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['2b55a846-4689-46d0-b812-ad5586eb566b']
    # sessions = ['2b55a846-4689-46d0-b812-ad5586eb566b', 'B26E7BE6-9549-44CB-98CB-7192329317E1',
    #             'F2C3F533-7DC3-4FF8-BF37-BB39381ECA91', 'D20E9CB5-B3DC-43EA-AF8E-B9C095D6D8C6',
    #             '80C42606-8E65-4BC9-926E-E94D712BE533', 'BC69DD34-1780-40A5-A9EB-BB8B394C4F21',
    #             'C82E0494-6037-4119-8BB0-950C5D89396D', '2152C645-6ACE-4B21-9D28-A8E17C4E9E17',
    #             '9e076b70-3e36-4c61-84fa-007e74b1891e', 'B55F5AF0-FF6E-47CC-8920-53B4E6C140E3']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
