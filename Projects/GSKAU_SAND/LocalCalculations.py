import pandas as pd
# from collections import OrderedDict
#
# from Trax.Utils.Logging.Logger import Log
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
# from Projects.GSKAU_SAND.SceneKpis.SceneCalculations import SceneCalculations
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id',
                                     Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


# if __name__ == '__main__':
#     LoggerInitializer.init('gskau calculations')
#     Config.init()
#     project_name = 'gskau'
#     # RUN for scene level KPIs
#     session_scene_map = OrderedDict([
#         ('12076D1D-FA3C-443C-951E-B4D0FBB80213', ['3825280F-A1A7-41A5-B90C-AA205A9A6D1E']),
#         ('3EFA8C57-0FEB-4CFD-A819-2521D8082DFE', ['24F335D0-648C-41C2-A60D-C26D82641928']),
#         ('4E5AA82E-C063-4B92-8C48-FA12761B6560', ['9ACBDCC5-24D4-4C0A-AB89-9B6219D8FE28']),
#         ('56C336BB-9797-4C8F-AC1C-D18E40218404', ['3DEC7521-93F1-4888-9631-4C4808932C30']),
#         ('BC981670-1F6B-485A-8695-A1FE552B07AE', ['2DADAFAE-7526-4AAE-A901-1AA623EA0BB9']),
#         ('F3AAC28E-87C9-4276-9390-BD579B13A64A', ['4902F86D-7FEF-40D2-B08A-4C87D3D708B1'])
#     ])
#
#     for session, scenes in session_scene_map.iteritems():
#         for e_scene in scenes:
#             print "\n"
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_scene_data(session, scene_uid=e_scene)
#             Log.info("**********************************")
#             Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
#             Log.info("**********************************")
#             output = VanillaOutput()
#             SceneVanillaCalculations(data_provider, output).run_project_calculations()
#             save_scene_item_facts_to_data_provider(data_provider, output)
#             SceneCalculations(data_provider).calculate_kpis()
# #     sessions = [
# #         '6DF2E0C8-8AE0-432A-AD3B-C2BE8F086E3B',
# #         '6520B138-780D-4AD1-95CF-8DA1727C4580',
# #         '6944F6F5-79D7-43FB-BE32-E3FAE237FA63',
# #         '6FDB6757-E3DA-4297-941A-8C1A40DD2E90',
# #         '9DC66118-F981-4D01-A6DD-1E181FA05507',
# #         'E0BE9853-B6C8-4B36-919C-5293BF52EF5B'
# #     ]
# #     # RUN FOR Session level KPIs
# #     for sess in sessions:
# #         Log.info("[Session level] Running for session: {}".format(sess))
# #         data_provider.load_session_data(sess)
# #         output = Output()
# #         Calculations(data_provider, output).run_project_calculations()
