
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVNL.Calculations import INBEVNLINBEVBECalculations


if __name__ == '__main__':
    LoggerInitializer.init('inbevnl-sand calculations')
    project_name = 'inbevnl'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'a68f4178-efa3-4c24-b7ae-ae77e7d40fa6'
        # '2b201dbc-1780-4800-84c5-7a4378578149'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        INBEVNLINBEVBECalculations(data_provider, output).run_project_calculations()
