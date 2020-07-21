from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKRU.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init()
    project_name = 'gskru'
    data_provider = KEngineDataProvider(project_name)
    session = 'fea0f508-cfa5-4968-be68-256575a122ea'

    # 'aca2d140-d7dc-4817-9a42-95d2a69c70ff'

    # 'e161137e-5943-427c-acc2-2ecd30d0e16d'
    # 'fea0f508-cfa5-4968-be68-256575a122ea'

    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
