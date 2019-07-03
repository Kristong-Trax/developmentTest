
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand calculations')
    Config.init()
    project_name = 'diageous-sand'
    sessions = [
        # Independent On:
        "4A1C1E22-20EB-4F1D-8D31-1E3EA45200A6",
        # Independent Off:
        "F85259FC-95EB-4DC9-A2F0-D6BCB355FEB8",
        # Open On:
        "FF9F50D8-7D90-456C-9690-C8D32B460B9E",
        # Open Off:
        "1FF9F838-5F2E-44D2-B1E4-3AE224CDCD8B",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
