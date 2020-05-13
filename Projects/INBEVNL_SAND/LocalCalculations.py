
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVNL_SAND.Calculations import INBEVNLINBEVBECalculations


if __name__ == '__main__':
    LoggerInitializer.init('inbevnl-sand calculations')
    project_name = 'inbevnl-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'e5222f16-9c78-47d0-8821-8d48adf255ee'
        # '63c8f3e5-dd45-48e5-93e5-7f10179b931b',
        # '5417f7ba-ee35-46a2-8918-f56671639467'
        # '2b201dbc-1780-4800-84c5-7a4378578149'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        INBEVNLINBEVBECalculations(data_provider, output).run_project_calculations()
