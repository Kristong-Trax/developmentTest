
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
        'f1c93102-4594-49d7-aae8-151dd65851b8',
        '73a0a5fa-b658-40e6-b718-8a50007f09ff',
#         '5f20ba65-63ae-4057-a8b0-3491accf7869',
        # '69b028e2-5119-4c11-a846-94ca29adede4',
        # 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
        # 'E45618DB-39E9-45A4-B541-054C85515A69'
        # '0eda0210-b4ed-461c-8b32-09bfddf0cab8',
        '036de825-5353-416d-8347-6b1c5cb1521f',
        # '1c7303e6-96bc-4360-822f-e00886701a1b',
        #'9ba32139-c2bd-4d36-96b3-6268628960ee'
        # '69b028e2-5119-4c11-a846-94ca29adede4',
        # 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
        # 'E45618DB-39E9-45A4-B541-054C85515A69'
        # '69b028e2-5119-4c11-a846-94ca29adede4',
        # 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
        # 'E45618DB-39E9-45A4-B541-054C85515A69'
        # '725524e8-2e9b-4c42-ace1-b2120d987f9e',

        # '725524e8-2e9b-4c42-ace1-b2120d987f9e',
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
