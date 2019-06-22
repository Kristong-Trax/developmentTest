
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.ALTRIAUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('altriaus calculations')
    Config.init()
    project_name = 'altriaus'
    sessions = [
        'A5629BC3-F3BD-4E16-BAA6-2DD046151C80',  # works!
        'F7011A7C-1BB6-4007-826D-2B674BD99DAE',  # works now, failed due to no smokeless section
        '46EAE20D-ED3B-41F7-97D2-E8DEAABD4270',  # works now, reduced point matching threshold because jumps were big
        # '3EB3EEF7-CA5B-45D6-9D99-1D230B86ECC1',  # works now, added functionality to save empty positions - smokeless
        # '6479848B-7A28-49F1-8568-B1CF4CC2A52F',  # works!
        # '64345CEA-5A15-4919-8FB2-8A06C0DEA7EE',  # works now, added functionality to account for weird locations in
        # # the longest shelf, and smokeless section
        # 'C1822AEA-A997-414B-ADEF-5EA28BD3B915',  # works now, was fixed by accounting for empty smokeless section
        # '2987F99F-579B-4DD5-83A3-DB9C5C75FED5',  # works now, was fixed by accounting for empty smokeless section
        # '3FF91AAC-6008-4B4E-BAD1-1220C1A2A45D'  # works now, a lot was done to fix this

    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
