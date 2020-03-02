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
        'B5A1D8EF-9A15-4B17-94C3-868CAC29FF0F',
        '8BD94E57-821B-4C0B-91A0-F6DD3DBF8413'
        # '811EF734-CDBB-4A64-AECC-1C4FCB869A9C',
        # 'ab611987-1355-4e62-b194-d868885e6597',
        # 'D3B6F6B2-7B36-4492-9E39-DD52C00D18EA',
        # '81D4223B-E87A-4321-8DF1-A54668937E5C',
        # '1FE24C62-5806-4E67-848F-6B5F7B95E824',
        # 'C20CAB48-304E-4EDF-9940-6F419A719055'
    ]
    for session in sessions:
        print("Running for {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
