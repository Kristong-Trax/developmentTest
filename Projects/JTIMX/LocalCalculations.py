
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JTIMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('jtimx calculations')
    Config.init()
    project_name = 'jtimx'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['5eecf0b7-5e05-48a3-9bf7-50bb545196ae']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
