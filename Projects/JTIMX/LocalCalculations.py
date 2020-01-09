
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JTIMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('jtimx calculations')
    Config.init()
    project_name = 'jtimx'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['2E68E1EF-03C4-4363-97B7-BE84897E0F7F']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
