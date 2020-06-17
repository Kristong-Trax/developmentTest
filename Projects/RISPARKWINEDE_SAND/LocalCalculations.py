
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.RISPARKWINEDE_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init('KEngine')
    project_name = 'risparkwinede-sand'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['A991BEAD-1F8F-4FCD-BC47-29BA2B1B0FCE', '5F0A4185-560B-432C-B349-0AA127B87C5B']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
