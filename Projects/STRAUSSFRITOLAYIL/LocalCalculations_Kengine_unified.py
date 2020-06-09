from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Apps.Services.KEngine.Handlers.UnifiedHandler import KEngineUnifiedHandler
from mock import MagicMock

if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init()
    project_name = 'straussfritolayil'

    # session_uid, session_id = '8B184570-DCA6-4A10-803A-06A35B66CA01', 22 # empty session
    session_uid, session_id = '865aa384-4b22-4f37-90cd-d9fd2d6b9072', 78

    message_session = {'event_name': 'SESSION_PROCESSED', 'timestamp': '', 'project_name': project_name,
                       'session_uid': session_uid, 'session_id': session_id, 'scene_ids': [], 'scene_uids': [],
                       'number_of_scenes': 4, 'attributes': {'ApproximateReceiveCount': 1}, 'wave_type': 'primary',
                       'wave_uid': ''}
    kenigineUnified = KEngineUnifiedHandler()
    # kenigineUnified._validate_message(message_session)
    kenigineUnified._process_message(message_session, None, MagicMock(), None)
