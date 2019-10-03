
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKSG_SAND2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gsksg calculations')
    Config.init()
    project_name = 'gsksg-sand2'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['ebadefeb-3e14-4225-8322-85ffe4757a77' , '879a605a-cca6-4d31-994c-39dd29e89b28',
                '4b860cc7-c546-4c30-8e71-6e8f4a095f68',
                'd9675289-f6c3-41ff-96ac-eb7224260e82',
                '5b4d90b5-723b-4b93-94e5-0d2fdbb32f35',
                'F85997CE-3961-4E42-B52A-33B1EE371080',
                '0AF772C9-4828-4617-918C-6298282B7B81',
                '8b8fb7d3-8d75-4aee-8f78-dc4d007378b7',
                '497D0D3C-5F23-4646-A837-C6D0A490BF2F',
                '9838f907-b1bf-44d7-969e-6c5e93ed480d',
                '0be8a2c5-f610-4f25-bfcc-5f392500c1df',
                'FE849CA4-694F-4F39-8B1E-1CDA5CCF7512',
                '944865F5-55A2-4BDE-B850-84968C4418E9',
                'F9EA8B2C-8C76-43E7-9AA0-D852F307736F']
    # session = 'fdae28d2-6e49-45d2-b357-cd9ca7a13de6'
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
