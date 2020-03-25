from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CARLSBRGHK.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('carlsberg calculations')
    Config.init()
    project_name = 'carlsbrghk'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '8BD94E57-821B-4C0B-91A0-F6DD3DBF8413',  # no scenes
        'B6FB5E83-BBBF-4988-A60D-2A5B62EA8CC5',
        'CC70B248-8C82-42F7-B3C5-C6A61B2B06AE',  # prev - B6FB5E83-BBBF-4988-A60D-2A5B62EA8CC5
        '17AB12D2-3D49-4846-8088-5CBB5C78D86E',  # prev - CC70B248-8C82-42F7-B3C5-C6A61B2B06AE
        '64CC70E0-3454-459B-8817-2BC78EDC4256',  # prev - 17AB12D2-3D49-4846-8088-5CBB5C78D86E
        'B5A1D8EF-9A15-4B17-94C3-868CAC29FF0F',  # prev no scenes - 64CC70E0-3454-459B-8817-2BC78EDC4256
        '4DAFCE77-82FA-4A51-B2D3-AD24D561237E',  # 0 msl cooler
    ]
    for session in sessions:
        print("Running for {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
