from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Apps.Services.KEngine.Handlers.UnifiedHandler import KEngineUnifiedHandler
from mock import MagicMock

if __name__ == '__main__':
    LoggerInitializer.init('straussfritolayil calculations')
    Config.init()
    project_name = 'straussfritolayil'
    session_uid = 'e20e0ba1-7be2-433a-9606-c6bf60e2c9e1'
    session_id = 19
    scene_uid = 'd188cff1-262c-4e16-8d28-191e037ac5e3'

    message_session = {'event_name': 'SESSION_PROCESSED', 'timestamp': '', 'project_name': project_name,
                       'session_uid': session_uid, 'session_id': session_id, 'scene_ids': [], 'scene_uids': [],
                       'number_of_scenes': 5, 'attributes': {'ApproximateReceiveCount': 1}, 'wave_type': 'primary',
                       'wave_uid': ''}


    kenigineUnified = KEngineUnifiedHandler()
    kenigineUnified._validate_message(message_session)
    kenigineUnified._process_message(message_session, None, MagicMock(), None)