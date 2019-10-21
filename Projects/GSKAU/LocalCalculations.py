
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.GSKAU.SceneKpis.SceneCalculations import SceneCalculations
from collections import OrderedDict

if __name__ == '__main__':
    LoggerInitializer.init('gskau calculations')
    Config.init()
    project_name = 'gskau'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        ('D7A0F7C1-A989-42D9-B7C8-B80620954883', ['6251D1CF-70BD-471A-B374-2E90FDB63A84']),
        ('C064F937-60D7-4A91-BCEE-2712FCC59182', ['88F9749F-B86D-486F-98E7-932029F6044B']),
        ('f2a3654a-82f6-4a12-8436-dfabe2cd8278', ['1d64a504-c700-493f-885a-d187d73af977']),
        ('899CA3D5-35C6-46FE-9354-72AF8BDA34E9', ['F59A4917-C4B2-4579-85CC-8EC9CD3BF4A9']),
        ('189c2806-79ab-4585-9b59-ae87dbbe9a93', ['e707278c-ebf5-49cb-a800-2e990cfac847']),
    ])
    for session, scenes in session_scene_map.iteritems():
        for e_scene in scenes:
            print "\n"
            data_provider = KEngineDataProvider(project_name)
            Log.info("**********************************")
            Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
            Log.info("**********************************")
            data_provider.load_scene_data(session_uid=session, scene_uid=e_scene)
            SceneCalculations(data_provider).calculate_kpis()
#     sessions = [
#         '6DF2E0C8-8AE0-432A-AD3B-C2BE8F086E3B',
#         '6520B138-780D-4AD1-95CF-8DA1727C4580',
#         '6944F6F5-79D7-43FB-BE32-E3FAE237FA63',
#         '6FDB6757-E3DA-4297-941A-8C1A40DD2E90',
#         '9DC66118-F981-4D01-A6DD-1E181FA05507',
#         'E0BE9853-B6C8-4B36-919C-5293BF52EF5B'
#     ]
#     # RUN FOR Session level KPIs
#     for sess in sessions:
#         Log.info("[Session level] Running for session: {}".format(sess))
#         data_provider.load_session_data(sess)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
