
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
# '69b028e2-5119-4c11-a846-94ca29adede4',
# 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
# 'E45618DB-39E9-45A4-B541-054C85515A69'
# '43913E06-9CFF-48C2-B768-F1924EB32D9C',
#         'ce010a22-c9bb-4763-827b-002efc4ec2d0'
#         '6a81037a-0090-43ab-bb13-71c4944925ba'
#         'fee2ab0a-d288-45c4-9635-6e9700c0f347',

        # '8e040506-972b-4bab-b677-ffd389a67249',
        # '3920742f-725b-4c28-8ff6-e2590e2e0508',
        '1c7303e6-96bc-4360-822f-e00886701a1b',
        # '9ba32139-c2bd-4d36-96b3-6268628960ee'
        '71e87540-36ff-4025-a7b4-e715f29b0ef6',

    ]

    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
