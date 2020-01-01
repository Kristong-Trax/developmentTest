from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CBCIL_SAND.Calculations import CBCILCalculations


if __name__ == '__main__':
    LoggerInitializer.init('cbcil-sand calculations')
    Config.init()
    project_name = 'cbcil-sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        '9bbeb997-c48d-4767-9af3-fcac276e37a0'
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CBCILCalculations(data_provider, output).run_project_calculations()
