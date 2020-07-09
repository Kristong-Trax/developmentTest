
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.LIONNZ.Calculations import Calculations


if __name__ == '__main__':
    Config.init()
    project_name = 'lionnz'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '4C359AE0-3D19-4F70-BAC2-5D136C157009'
        # '35DCB86F-3E35-4F24-827C-F51112DC2420',
        # '9F36C933-BE8A-4BE5-9473-E23EFFDFEF63',
        # '6C388114-B8E5-4262-952D-87207BCB5C13',
        # '0737C746-A8FF-4896-8395-D3D130A81AE7',
        # 'D28D70F3-CC2C-41CE-9F35-8232E2D85513',
        # '36A72CCF-3429-4946-BCE2-A11B1AB3DE4F',
        # '66C7159B-4D46-45A2-A185-D88055897E2A',
        # 'CA42B047-808B-4605-91C7-F8127FD66043',
        # '0BDF9C70-CDC6-43BC-94D7-C6126995EDF8',
        # '29963E40-DFAA-496D-BE1D-7F2C308078E4',
        # 'B0F794CD-15B9-4A7D-B4D7-3A2C6F6FBD8B',
        # '086B9A65-9A68-4EA0-B665-95E1C04F9377',
        # 'E9B49ED2-7C3D-40DE-A0FB-1C79FD3C95AF',
        # 'DE2107CD-3E65-4E8B-B8CD-B52B2961DC40',
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
