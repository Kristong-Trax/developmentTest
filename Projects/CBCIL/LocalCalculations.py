from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CBCIL.Calculations import CBCILCalculations


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init()
    project_name = 'cbcil'
    data_provider = KEngineDataProvider(project_name)
    session_uids = ['ff27c9ab-6c61-4137-bc15-d6049774ace1']
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CBCILCalculations(data_provider, output).run_project_calculations()
