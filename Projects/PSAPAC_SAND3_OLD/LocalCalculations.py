
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PSAPAC_SAND3.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('psapac-sand3 calculations')
    Config.init()
    project_name = 'psapac-sand3'
    data_provider = KEngineDataProvider(project_name)
    session = '6adb054b-0b49-45dd-9a6a-12e919252f82'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
