
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.ALTRIAUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('altriaus calculations')
    Config.init()
    project_name = 'altriaus'
    sessions = [
        # 'A5629BC3-F3BD-4E16-BAA6-2DD046151C80',  # works!
        # 'F7011A7C-1BB6-4007-826D-2B674BD99DAE',  # works now, failed due to no smokeless section
        # '46EAE20D-ED3B-41F7-97D2-E8DEAABD4270',  # works now, reduced point matching threshold because jumps were big
        # '3EB3EEF7-CA5B-45D6-9D99-1D230B86ECC1',  # works now, added functionality to save empty positions - smokeless
        # '6479848B-7A28-49F1-8568-B1CF4CC2A52F',  # works!
        # '64345CEA-5A15-4919-8FB2-8A06C0DEA7EE',  # works now, added functionality to account for weird locations in
        # # the longest shelf, and smokeless section
        # 'C1822AEA-A997-414B-ADEF-5EA28BD3B915',  # works now, was fixed by accounting for empty smokeless section
        # '2987F99F-579B-4DD5-83A3-DB9C5C75FED5',  # works now, was fixed by accounting for empty smokeless section
        # '3FF91AAC-6008-4B4E-BAD1-1220C1A2A45D'  # works now, a lot was done to fix this
        # '6F94147F-E64C-48BC-8467-D610989CC5C9',
        # '873A646F-1EB1-4998-94CA-000B5AF88032',
        # '48D9F166-F4AF-493C-90C3-E6115E032362',
        # 'AB99B089-FF06-468D-B978-7CB32ECBFFC0',
        # '61C50C83-6A0F-4557-BA9B-EC64D4968118',
        # 'E8464DB4-CEF7-4ADA-85F4-5B60CDDF5FF8',
        # '78BC2765-C7B1-4FB3-8DFB-D82E93EBE02C',
        # 'DCC3F220-39A2-4DD2-AFE1-D6E766322FE4',
        # 'E27BEAB2-721D-44AB-9A6C-80F30C5B9CFD',
        # 'D918336C-B626-4B7C-9CFD-00E99FC1A03C',
        # '877E5A64-81FA-4995-9980-5CF0F20A4DE2',
        # 'C2704C6D-87D5-42EF-8D29-D134AB524471',
        'EBE1F5D0-BC2E-4B58-AE90-1E3DDD6523D4',
        'B9726241-0FC1-4073-ABA6-D7A1AEFFEA42',
        '7C54538B-AD78-45D9-9B37-D61549C0B317',
        'E999EB3F-6AFD-4974-929C-C6859C9DBA30',
        'E999EB3F-6AFD-4974-929C-C6859C9DBA30',
        '497A5772-F514-4216-A94F-78C76FB05722',
        '06660FDB-E3C1-47DD-B351-2F620340C948'

    ]
    for session in sessions:
        print('===================={}===================='.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
