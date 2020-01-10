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
        '3c9bcb32-7775-44cd-9e2a-24e9b896a707'
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CBCILCalculations(data_provider, output).run_project_calculations()
