
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand calculations')
    Config.init()
    project_name = 'diageous-sand2'
    sessions = [
        # Independent On:
        "4A1C1E22-20EB-4F1D-8D31-1E3EA45200A6",
        "3E09D306-5805-4553-9FCC-A952D4F46E4C",
        # Independent Off:
        "F85259FC-95EB-4DC9-A2F0-D6BCB355FEB8",
        "74B7E062-01D9-496E-B046-9EBF479095DB",
        "3EA574D2-B7AB-41B4-8B52-DC87C8ED5E98",
        # Open On:
        "FFEA78F0-AEED-48A5-97F9-374DB552AF73",
        "FF9F50D8-7D90-456C-9690-C8D32B460B9E",
        # Open Off:
        "FF35FDC6-462D-4EEC-987A-5627CFA28316",
        "FFB6311D-17F9-4A90-AE68-E3B1504E4592",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
