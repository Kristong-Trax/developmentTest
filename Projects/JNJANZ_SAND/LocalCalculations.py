from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJANZ_SAND.Calculations import JNJANZ_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('JNJANZ_SAND calculations')
    Config.init()
    project_name = 'jnjanz-sand' #'JNJANZ_SAND'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        # "4fccf7a9-108b-4ec2-b24b-70bec737b576",
        # "ac6a9efb-a6f7-4d03-a1fd-b7ff9ddf17a2", # prev
        # "d317cc6d-8083-407f-8693-e21f5b474412",
        # "f753f5ce-d89f-485a-a9d9-f926fd029297"  # prev
        ]
    # 'CE9BD14E-DBF1-4F82-BEE4-E1D61F0B211F'
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJANZ_SANDCalculations(data_provider, output).run_project_calculations()
