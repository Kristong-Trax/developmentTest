
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous calculations')
    Config.init()
    project_name = 'diageous'
    sessions = [
        '9740EC9E-5D5F-49B0-A1DE-309C849AD804', '8AC878A0-17C3-4764-9388-D245F1F6810B'
        # National On:
        #"3F6CFD91-0D38-4A09-AD0D-510B58868296",
        # Independent On:
        # "4A1C1E22-20EB-4F1D-8D31-1E3EA45200A6",
        # # Independent Off:
        # "F85259FC-95EB-4DC9-A2F0-D6BCB355FEB8",
        # # Open On:
        # "FF9F50D8-7D90-456C-9690-C8D32B460B9E",
        # # Open Off:
        # "1FF9F838-5F2E-44D2-B1E4-3AE224CDCD8B",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
