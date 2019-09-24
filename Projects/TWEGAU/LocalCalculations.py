from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


if __name__ == '__main__':
    project_name = 'twegau'
    # SCENE LEVEL LOCAL CALC -- WATCH THE IMPORTS ADDED BELOW
    # from Projects.TWEGAU.SceneKpis.SceneCalculations import SceneCalculations
    # from Trax.Utils.Logging.Logger import Log
    # from collections import OrderedDict
    # session_scene_map = OrderedDict([
    #     ("2C043D85-610D-4BC6-A1FC-A83A826B6D89", [390119, 390918, 390935]),
    #     ("7BD428D1-59FE-4563-BA54-F1C8E8DAA72D", [390606, 390608, 390613, 390618]),
    #     ("D2587899-84AF-4E4D-99F3-C61D35A0A3FC", [388902, 389429, 390328, 390329, 390345]),
    #     ("2F283C38-4A79-4AAD-AC40-43AFA81E63E6", [390871, 390882, 390891, 390898, 390901]),
    #     ("D7FF4A10-13E7-47B0-BE34-F0713215531C", [390670, 390671, 390672, 390689, 390698, 390705, 390712, 390714]),
    #     ("FDBCF9A1-A0D4-430A-AE6E-3428CBDDAE23", [391777, 391780, 391785, 389768, 389797, 389845, 389853, 389871]),
    #     ("3DB6D8B1-3282-4A73-8539-1C2B47CEFB27", [391000, 391024, 391032, 391041, 391059, 391080, 391089, 391095,
    #                                               391100, 391102, 391104, 391106, 391108]),
    # ])
    # for session, scenes in session_scene_map.iteritems():
    #     for e_scene in scenes:
    #         print "\n"
    #         data_provider = KEngineDataProvider(project_name)
    #         Log.info("**********************************")
    #         Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
    #         Log.info("**********************************")
    #         data_provider.load_scene_data(session, e_scene)
    #         SceneCalculations(data_provider).calculate_kpis()

    # SESSION LEVEL LOCAL CALC -- WATCH THE IMPORTS ADDED BELOW
    from Trax.Algo.Calculations.Core.DataProvider import Output
    from Projects.TWEGAU.Calculations import Calculations
    LoggerInitializer.init('twegau calculations')
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        "000E4051-2111-4220-9991-3D3FA1102540",  # store number 1 => '556032'
        "00B36414-40EE-4F43-A277-D17969F88567",  # store number 1 => '604644'
        "03CA5819-063C-452D-9F14-99370BCE9279",  # store number 1 => '602949'
        "04899E1E-8714-433D-A9AA-8B665412E75B",  # store number 1 => '523570'
        "0510B1BD-409A-4ACD-A406-7BFD9C4F7897",  # store number 1 => '519567'
        "094AE49F-81AD-4E37-A247-F9F0641BFBDF",  # store number 1 => '516121'
        "0A14CB41-C8DA-458F-A6D9-630FEE94EE84",  # store number 1 => '603147'
        "0CE8A970-A014-457C-AF5D-81387D0688F7",  # store number 1 => '556032'
        "0D09EC05-AC0F-4538-A293-10F5A501B9AB",  # store number 1 => '522119'
        "109E18B6-BDE5-48D9-892B-B6D5C6085F8B",  # store number 1 => '505620'

    ]

    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        print "*******************************"
