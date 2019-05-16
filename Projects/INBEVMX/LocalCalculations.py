
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('inbevmx calculations')
    Config.init()
    project_name = 'inbevmx'
    data_provider = KEngineDataProvider(project_name)
    list_sessions = [
                    '7fc675ab-36bc-4172-a770-ccd54218d446',
                    '51b59c2c-0fad-429a-a13f-9b7487d2d2f4',
                    '763b2702-25dd-48f2-855c-f5c0522234a2',
                    '22885789-cf8e-43a3-8fef-611e483b8db3',
                    '4d74b90b-c2f0-4776-a6eb-792feeee3602',
                    'b7ee639f-4fe3-497b-9473-9af97691b655',
                    '9faf34a0-7b90-4420-b1c7-e5ac705f6c11',
                    '73748ac0-0d45-4861-8be7-a01948fdbb87',
                    '74ef051e-2e27-4a0a-abab-a44cd63d0c68',
                    'be4195b6-6e7b-479a-9c90-33b297ed9f60'
    ]
    output = Output()
    for session in list_sessions:
        data_provider.load_session_data(session)
        Calculations(data_provider, output).run_project_calculations()