from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Apps.Services.KEngine.Handlers.UnifiedHandler import KEngineUnifiedHandler
from mock import MagicMock

if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init()
    project_name = 'straussfritolayil'

    # session_uid, session_id = '8B184570-DCA6-4A10-803A-06A35B66CA01', 22 # empty session
    sessions = [
                {"session_uid": '74b0b057-f064-4954-a296-bcb2f9f53cce', "session_id": 48556},
                {"session_uid": "d386503d-f3bf-4167-9f7e-2a73ab494b45", "session_id": 49696},
                {"session_uid": "fdb00c3e-11b9-4999-ac54-33fecf3f4ef4", "session_id": 23818},
                {"session_uid": "ced98c6c-6151-40bb-adcd-b87f81978909", "session_id": 37397},
                {"session_uid": "fdb00c3e-11b9-4999-ac54-33fecf3f4ef4", "session_id": 23818},
                {"session_uid": "b99e0244-727a-4c23-865e-685e6d6fedc7", "session_id": 26903},
                {"session_uid": "183abf3c-5992-4c4b-8ed4-0d6daa35c7a1", "session_id": 27196},
                {"session_uid": "5eaae72d-e136-4333-a798-1576879fa8ac", "session_id": 26678},
                {"session_uid": '5b7f907d-2b38-46c3-8eaf-fd3145150632', "session_id": 26140},
                {'session_uid': 'f19291d8-1f67-462f-ba46-b8837d421ab9', 'session_id': 22809},
                {'session_uid': '626d0d90-9103-469d-8ab0-3d428054f664', 'session_id': 22789},
                {'session_uid': '865aa384-4b22-4f37-90cd-d9fd2d6b9072', 'session_id': 78}]

    for session in sessions:
        session_uid = session['session_uid']
        session_id = session['session_id']
        message_session = {'event_name': 'SESSION_PROCESSED', 'timestamp': '', 'project_name': project_name,
                           'session_uid': session_uid, 'session_id': session_id, 'scene_ids': [], 'scene_uids': [],
                           'number_of_scenes': 1, 'attributes': {'ApproximateReceiveCount': 1}, 'wave_type': 'primary',
                           'wave_uid': ''}
        kenigineUnified = KEngineUnifiedHandler()
        # kenigineUnified._validate_message(message_session)
        kenigineUnified._process_message(message_session, None, MagicMock(), None)
