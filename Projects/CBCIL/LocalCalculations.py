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
        'a21d1738-7cb6-4f83-9274-7109b483ed09'
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CBCILCalculations(data_provider, output).run_project_calculations()
