
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand2 calculations')
    Config.init()
    project_name = 'diageous-sand2'
    sessions = [
        # '95A898F6-EE82-4C8E-84C3-C0631BAE8083',
        # National On:
        #"3F6CFD91-0D38-4A09-AD0D-510B58868296",
        # Independent On:
        "b660518b-bcb8-470e-b9fc-0528b38c46bf",
        # # Independent Off:
        # "F1798F68-1B61-4679-A989-FB0321F2A02F",
        # # Open On:
        # "FF9F50D8-7D90-456C-9690-C8D32B460B9E",
        # # Open Off:
        # "1FF9F838-5F2E-44D2-B1E4-3AE224CDCD8B",
    ]
    sessions = [
        '983DF4A8-8380-416F-8AFB-15FFCD067CCC',
        'D117A929-78A8-4ECE-9EC4-B24A76E0A564',
        '88481885-13F1-4F6B-9BF9-73BF38BF7DB7',
        '3F56C98C-1948-4DFE-B65F-7AD7C178CE91'
    ]
    sessions = [
        'f1fe8759-a7ae-4d30-894c-b34efe0808cd'
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
