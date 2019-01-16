from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOBEERUS.Calculations import Calculations

sessions = [
    'C682A0F1-0694-40DF-B20C-E5A3E5900E9D',
    'EBB377F6-AAE9-4B70-AC0E-FAD8D891DB29',
    'C682A0F1-0694-40DF-B20C-E5A3E5900E9D',
    'E3426DF8-D8C5-450B-B3CD-654632021BD2'
]

if __name__ == '__main__':
    LoggerInitializer.init('diageobeerus calculations')
    Config.init()
    project_name = 'diageobeerus'
    data_provider = KEngineDataProvider(project_name)
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
