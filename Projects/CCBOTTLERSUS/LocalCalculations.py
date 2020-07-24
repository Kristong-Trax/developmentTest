from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS.Calculations import CCBOTTLERSUSCalculations
from Projects.CCBOTTLERSUS.KPIGenerator import CCBOTTLERSUSGenerator
from Projects.CCBOTTLERSUS.MSC.KPIToolBox import MSCToolBox
from KPIUtils_v2.DB.CommonV2 import Common



if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'ccbottlersus'

    # # MSC
    # sessions = [
    #     'FC51FAC6-4EBB-4C9B-AC1B-F72052442DDE',
    #     'E79B5B80-BAA2-4FA0-8C1F-594269B39457',
    #     'E86F80DE-62C2-44AB-9949-80E520BCB3B2',
    #     'F05079E5-11C4-4289-B5AE-5B8205594E15',
    #     'dc322cc1-bfb7-4f2b-a6c3-c4c33a12b077'
    # ]
    # # Liberty
    # sessions = [
    #     'fe6e86a5-e96c-4ed1-b285-689ee8da393c',
    #     'FAB57A4E-4814-4B74-A521-53A003864D06',
    #     'BE9F0199-17B6-4A11-BA97-97751FE6EE0E',
    #     'f6c0247d-64b4-4d11-8e0b-f7616316c08f'
    # ]
    sessions = ['019B32A0-FE74-43A3-B375-796CCBD797CB']

    sessions = [
        'FDC55FB2-C731-4535-B132-2ABE3678E8D0',
        'F661BEB9-6B29-4640-B8FB-BA8C9F3C0209',
        'FAB121A6-72BC-4079-A8B0-C17BA244C2BE',
        'F51B7530-6A86-411E-A23F-9B831DB3DCC7',
        'FF146A9B-5444-45F4-AC07-C10B18B9F0C7'
    ]

    sessions = [
        'f0fa96c0-1870-4d48-ba7b-76a8ececcd6a',
        'd54945e1-be4d-4fd7-a363-d0f78432c322',
        'ffdd7097-6081-4a50-9cea-2739d647341d'
    ]

    sessions = [
        '7f474bd8-cd5e-413c-90e8-0be771f03f1c',
        '56c14363-dc7c-43a1-b51c-a6b7b3215595',
        '826ace9e-abec-4660-9b65-96d7a4604bf8',  # Contact Center
    ]

    sessions = [
        'fd4b1898-618c-4c74-a0f9-f8d2e12c2bd9'
    ]

    for session in sessions:
        print('***********************************************************************************')
        print('_______________________ {} ____________________'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        # common = Common(data_provider)
        CCBOTTLERSUSGenerator(data_provider, output).main_function()
        # MSCToolBox(data_provider, output, common).main_calculation()
        # common.commit_results_data()
