
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.LIONNZ.Calculations import Calculations


if __name__ == '__main__':
    Config.init()
    project_name = 'lionnz'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        # '02e04d93-5b92-4e04-b1a3-54c6fba20bb7',  # FSOS has non LION
        '00675BD2-1633-4B3A-84DF-F0EC22FC9A04',  # categ error
        # '6B1FC097-5406-4A42-BA78-FA3904675B5B',  # categ error
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
