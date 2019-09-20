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
        ("BAE5D1D0-B341-45AE-BA99-889B611266C8", [23610]),  # no targets match [multi bay]
        ("82CFBFC1-BDD9-4CB8-8B39-87C11B6FF62E", [23613]),  # multi bay
        ("060C5E25-2B5B-4F95-BA99-50325E866407", [23598]),  # multi bay
        ("749AD6BB-54DA-4727-9852-A6D503BECB96", [23595]),  # multi bay
        ("26595CE1-1F43-4F50-9918-45EB1B34DDCE", [23604]),  # multi bay
        ("286DACD0-A5AA-42AD-BC7E-6BFC4A0135B3", [23607]),  # multi bay
        ("103CDB86-69BF-4B4E-9B34-E32B0FA8B235", [23601]),
        ("59EC0A21-500E-4E78-88AC-5B759FB39731", [23586]),
        ("9BF79D4B-A15F-4EF3-B687-499ACC51669B", [23589]),
        ("8F9D4831-2DFD-4F36-B6A5-BA3273CDEBBB", [23592]),
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
