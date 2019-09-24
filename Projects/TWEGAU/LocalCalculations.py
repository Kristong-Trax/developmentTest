from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log
from collections import OrderedDict


if __name__ == '__main__':
    LoggerInitializer.init('twegau calculations')
    project_name = 'twegau'
    data_provider = KEngineDataProvider(project_name)

    # SCENE LEVEL LOCAL CALC -- WATCH THE IMPORTS ADDED BELOW
    from Projects.TWEGAU.SceneKpis.SceneCalculations import SceneCalculations
    session_scene_map = OrderedDict([
        ("2C043D85-610D-4BC6-A1FC-A83A826B6D89", [390119, 390918, 390935]),
        ("7BD428D1-59FE-4563-BA54-F1C8E8DAA72D", [390606, 390608, 390613, 390618]),
        ("D2587899-84AF-4E4D-99F3-C61D35A0A3FC", [388902, 389429, 390328, 390329, 390345]),
        ("2F283C38-4A79-4AAD-AC40-43AFA81E63E6", [390871, 390882, 390891, 390898, 390901]),
        ("D7FF4A10-13E7-47B0-BE34-F0713215531C", [390670, 390671, 390672, 390689, 390698, 390705, 390712, 390714]),
        ("FDBCF9A1-A0D4-430A-AE6E-3428CBDDAE23", [391777, 391780, 391785, 389768, 389797, 389845, 389853, 389871]),
        ("3DB6D8B1-3282-4A73-8539-1C2B47CEFB27", [391000, 391024, 391032, 391041, 391059, 391080, 391089, 391095,
                                                  391100, 391102, 391104, 391106, 391108]),
    ])
    for session, scenes in session_scene_map.iteritems():
        for e_scene in scenes:
            print "\n"
            data_provider = KEngineDataProvider(project_name)
            Log.info("**********************************")
            Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
            Log.info("**********************************")
            data_provider.load_scene_data(session, e_scene)
            SceneCalculations(data_provider).calculate_kpis()

    # SESSION LEVEL LOCAL CALC -- WATCH THE IMPORTS ADDED BELOW
    # from Trax.Algo.Calculations.Core.DataProvider import Output
    # from Projects.TWEGAU.Calculations import Calculations
    # sessions = [
    #     'F8AF5B91-7CA1-4134-AA0D-DEE08547933C',
    #     '4138394A-F64F-421D-AC10-0876F5EEE4FE',
    #     '184741F1-93ED-4A37-8217-77338210F009'
    # ]
    #
    # for session in sessions:
    #     print "Running session >>", session
    #     data_provider.load_session_data(session)
    #     output = Output()
    #     Calculations(data_provider, output).run_project_calculations()
    #     print "*******************************"
