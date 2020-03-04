
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCNAYARMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccnayarmx calculations')
    Config.init()
    project_name = 'ccnayarmx'
    data_provider = KEngineDataProvider(project_name)
    session_list = [
#         '5f20ba65-63ae-4057-a8b0-3491accf7869',
        # '90004fbd-58dd-418f-a359-f2605134291c',
        # '677d0628-9566-4760-8702-821882f74665',
        # 'f6f086d8-b269-4f2d-8229-144af3b1edf8',
        # '7e117743-0448-447b-9ad3-7c895ca8a0b7',
        # '543f7eff-309b-419c-8f39-931cc5cbcba6',
        # '199eda17-4fbb-4c23-b02d-8f3f47e079d9',
        ]


    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
