from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCLIBERTYUS.Calculations import CCLIBERTYUSCalculations
from Projects.CCLIBERTYUS.MSC.KPIToolBox import MSCToolBox
from KPIUtils_v2.DB.CommonV2 import Common

if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'cclibertyus'

    # MSC
    sessions = [
        'FC51FAC6-4EBB-4C9B-AC1B-F72052442DDE',
        'E79B5B80-BAA2-4FA0-8C1F-594269B39457',
        'E86F80DE-62C2-44AB-9949-80E520BCB3B2',
        'F05079E5-11C4-4289-B5AE-5B8205594E15',
        'dc322cc1-bfb7-4f2b-a6c3-c4c33a12b077'
    ]
    # Liberty
    sessions = [
        'fe6e86a5-e96c-4ed1-b285-689ee8da393c',
        'FAB57A4E-4814-4B74-A521-53A003864D06',
        'BE9F0199-17B6-4A11-BA97-97751FE6EE0E',
        'f6c0247d-64b4-4d11-8e0b-f7616316c08f'
    ]

    sessions = ['D51D704F-6274-49EF-8243-FECE54DF25B8',
                '1385B157-3E42-4D59-9768-CB1374388B1A',
                '96aebd09-7bd8-4263-a0ac-ecbbdc2bb02a',
                'f9ddea0f-07de-4b0e-a40a-c18f986912c1']

    # geolocation session (fails for being 2 KM away)
    sessions = ['8f2fdbe3-0654-4c0d-ad76-30d7407b7797']

    for session in sessions:
        print('***********************************************************************************')
        print('_______________________ {} ____________________'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        # common = Common(data_provider)
        # MSCToolBox(data_provider, output, common).main_calculation()
        CCLIBERTYUSCalculations(data_provider, output).run_project_calculations()
        # common.commit_results_data()

