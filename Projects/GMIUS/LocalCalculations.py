from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GMIUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gmius calculations')
    Config.init()
    project_name = 'gmius'

    sessions = [
                'c85d058d-6fcf-4865-830b-ab6563230096',
                'd44b436c-7df6-4a64-9197-b2c441dc5bef',
                'ccbfc4c6-7ad8-4dcb-b3bc-833b9d6d5a32',
                '8b9d2a2b-1272-48ef-b50a-2a745f1e0829',
                '8968eeb4-b487-42de-8939-02cbfa433a0e',
                '09a94f4a-038b-4f1c-8cbc-637d5ea08fdc',
                ]

    # sessions = ['c85d058d-6fcf-4865-830b-ab6563230096']

    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

