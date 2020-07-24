
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand2 calculations')
    Config.init()
    project_name = 'diageous-sand2'
    sessions = [
        # '95A898F6-EE82-4C8E-84C3-C0631BAE8083',
        # National On:
        #"3F6CFD91-0D38-4A09-AD0D-510B58868296",
        # Independent On:
        "b660518b-bcb8-470e-b9fc-0528b38c46bf",
        # # Independent Off:
        # "F1798F68-1B61-4679-A989-FB0321F2A02F",
        # # Open On:
        # "FF9F50D8-7D90-456C-9690-C8D32B460B9E",
        # # Open Off:
        # "1FF9F838-5F2E-44D2-B1E4-3AE224CDCD8B",
    ]
    sessions = [
        '983DF4A8-8380-416F-8AFB-15FFCD067CCC',
        'D117A929-78A8-4ECE-9EC4-B24A76E0A564',
        '88481885-13F1-4F6B-9BF9-73BF38BF7DB7',
        '3F56C98C-1948-4DFE-B65F-7AD7C178CE91'
    ]
    sessions = [
        'b660518b-bcb8-470e-b9fc-0528b38c46bf'
    ]
    sessions = [
        # 'EE612930-FE23-4AA5-A14D-FA602274A14B',
        # 'D55CEA7C-8A5C-4515-9E9C-3F6ACF784E65',
        # 'D117A929-78A8-4ECE-9EC4-B24A76E0A564',
        # 'CB1B5FE5-F346-4B8C-A28B-EFE23BF37EA4',  # index out of range
        # 'C237CC2D-2159-49B5-9EAF-423EC5012BE9',
        'A20CEF44-78B1-434F-989A-107AA5C5B4C3',  # empty arg
        '9B69C652-9F2B-4149-A625-0620E235610F',
        '983DF4A8-8380-416F-8AFB-15FFCD067CCC',
        '9445445A-D92C-4CD0-BC88-6DD302E3D84D',
        '8CE4CE0F-D6D7-432A-A404-307BA506AA6C',
        '74F1E39A-7955-4AFD-91F0-54D5B9112F33',
        '6F760D56-E132-4B1E-B43B-17172A54C9A7',
        '5B7F5040-38B6-442A-8303-E94A4E7CF6C4',
        '3F56C98C-1948-4DFE-B65F-7AD7C178CE91',
        '3B6F8B8A-5C04-40AD-B690-D96CB9D372E9',
        '31A09D88-2881-4FEB-A0DF-7D4D09CE7186',
        '30111DD3-D8FF-4147-AB93-37E5CB98B303',
        '18465B55-B03C-45FB-A15A-B4DEFAA6CE6B',
        '1403231E-CEB7-4DC5-87C4-5CCE4AF30EE1',
        '0FBC3C4E-CAF9-4CA2-B1D3-8DD733200CD5',
        '0D7389CE-E159-49C2-B2CE-39788B1D3F67',
        '000B6C50-68D6-4A1F-BA87-7A0FFC0BB241',
    ]
    sessions = [
        'CB1B5FE5-F346-4B8C-A28B-EFE23BF37EA4'
    ]
    # menu sessions
    sessions = [
        'da59bdc3-97fc-4828-a7b2-55531a4cb92d',
        'ced6fabd-cb30-4c23-8183-091a12012900'
    ]
    # display share sessions
    sessions = [
        '7285B363-889A-4116-A551-4F8AD23B9B16'
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
