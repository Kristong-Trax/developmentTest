
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCAAU.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccaau calculations')
    Config.init()
    project_name = 'ccaau'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['67547BB4-71BB-4B82-BD64-64A3824F2811',
                'E69FE0A1-88C3-44D9-962C-4ACF3AB903A4',
                '96BF8BBE-897B-484B-B2EB-2C1044A8BB3A',
                '48238A03-7F5F-4A2F-A112-1BCB17A18914'
                ]
    for sess in sessions:
        data_provider.load_session_data(sess)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
