
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

        '8e040506-972b-4bab-b677-ffd389a67249',
        '8fdc9d71-b173-4bde-aa5c-b2a0fa296d98',
        '018f437c-718e-4f15-9e2f-fd03bb3b4404',
        # '9ba32139-c2bd-4d36-96b3-6268628960ee'
        'a92dbe43-4270-47cd-ab0c-2f134d081064'
    ]

    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
