from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU.Calculations import CCRUCalculations


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init()
    project_name = 'ccru'
    data_provider = KEngineDataProvider(project_name)
    session_uids = \
        [

            '013a5e8f-df5d-4744-b5c4-e77c5ed58617'

        ]
    for session in session_uids:
        print(session)
        data_provider.load_session_data(session)
        output = Output()
        CCRUCalculations(data_provider, output).run_project_calculations()

