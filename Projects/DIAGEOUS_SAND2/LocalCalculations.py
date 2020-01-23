
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand2 calculations')
    Config.init()
    project_name = 'diageous-sand2'
    sessions = [
        # # Independent On:
        # "4A1C1E22-20EB-4F1D-8D31-1E3EA45200A6",
        # # Independent Off:
        # "F85259FC-95EB-4DC9-A2F0-D6BCB355FEB8",
        # # Open On:
        # "FF9F50D8-7D90-456C-9690-C8D32B460B9E",
        # # Open Off:
        # "1FF9F838-5F2E-44D2-B1E4-3AE224CDCD8B",
        'ecafd598-3b1a-444b-9e3c-788f7fa7c8f4',
        'e8963acf-4d0e-4bab-ba0a-e6895b1d33c8',
        'd0f536ef-dfeb-4439-9e61-23b2bbc9c343',
        'BB542AE4-2E36-4A6B-A15B-0104B4E077EB',
        'BADA0F4C-ECB6-4C8E-8DEF-37EDD4B9AA56',
        '9505e6b7-92b5-4030-bbdf-16a81144374f',
        '8A6EF6C1-B9B2-44BB-A4FE-27BFE8129ED4',
        '7F98E78A-324D-4836-B90F-D52F3CCDC5F6',
        '74A81945-987E-43E5-A4E3-EF5B0CF7854F',
        '6cf9c59b-39bb-4453-b600-6c5ad976066a'
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
