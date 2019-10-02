
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLEIL_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestleil-sand calculations')
    Config.init()
    project_name = 'nestleil-sand'
    data_provider = KEngineDataProvider(project_name)
    session = "430fbbc6-7682-4266-b28f-564dd692c26a"
    # session = "16dd8c3f-967b-442b-a9e4-7219eeddcd45"
    # session = "e902743d-5892-4b8f-aae8-618dc0b19ab0"
    # session = 'bc63d5f7-7d94-4c68-8843-2d58a737d442'
    # session = '0c3a387c-6212-4da8-9a75-360fe92d05b0'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
