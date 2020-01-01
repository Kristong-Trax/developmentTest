from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CBCIL.Calculations import CBCILCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CBCIL calculations')
    Config.init()
    project_name = 'cbcil'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        'ff600e2b-f155-47c0-8ef3-d0e22e00da73'
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CBCILCalculations(data_provider, output).run_project_calculations()
