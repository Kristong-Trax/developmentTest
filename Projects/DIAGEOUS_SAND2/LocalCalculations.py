
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
        # "4A1C1E22-20EB-4F1D-8D31-1E3EA45200A6", "3E09D306-5805-4553-9FCC-A952D4F46E4C",
        # # Independent Off:
        "F85259FC-95EB-4DC9-A2F0-D6BCB355FEB8", "74B7E062-01D9-496E-B046-9EBF479095DB",
        # Open On:
        # "FFEA78F0-AEED-48A5-97F9-374DB552AF73", "FF9F50D8-7D90-456C-9690-C8D32B460B9E",
        # Open Off:
        # "FF35FDC6-462D-4EEC-987A-5627CFA28316",
        # "FFB6311D-17F9-4A90-AE68-E3B1504E4592"

        # # "F85259FC-95EB-4DC9-A2F0-D6BCB355FEB8",
        # "E5C55CC1-123A-4778-BD08-C6E517F182F0",
        # "CAAE7233-4FE2-48CC-998D-D2DAEC3A5A71",
        # "C4086839-335F-41B4-B6CE-59B452C3E609",
        # "C2899C23-FD1A-4C29-976F-72206803F487",
        # "8B3FF9DC-FB05-455D-B235-C9F6F6756BF4",
        # # "74B7E062-01D9-496E-B046-9EBF479095DB",
        # "71883BC3-57CB-420B-A127-C8F2F4E9C2AE",
        # "70E645B5-964C-455D-8E43-2DB96B578AB3",
        # "61B326C8-4013-472E-8DC2-24A15F7012EA",
        # "5B81E09C-B246-488E-939B-14922E14B5D7",
        # # "4A1C1E22-20EB-4F1D-8D31-1E3EA45200A6",
        # "45A474AD-7562-43F0-B365-7D253B843CC5",
        # "3EA574D2-B7AB-41B4-8B52-DC87C8ED5E98",
        # "3EA3AB41-9BEF-4C84-925D-61037B7459A0",
        # # "3E09D306-5805-4553-9FCC-A952D4F46E4C",
        # "126CC274-B733-4154-BEF3-9E096E6B874A",
        # "0EB78184-A0AD-4CC1-B1A7-1CF7E674264F"
        "CAAE7233-4FE2-48CC-998D-D2DAEC3A5A71",
        "61B326C8-4013-472E-8DC2-24A15F7012EA",
        "5B81E09C-B246-488E-939B-14922E14B5D7",
        "4A1C1E22-20EB-4F1D-8D31-1E3EA45200A6",
        "3E09D306-5805-4553-9FCC-A952D4F46E4C"
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
