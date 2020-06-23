from datetime import datetime
from Trax.Apps.Services.KEngine.Handlers import UnifiedHandler
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from mock import MagicMock
from Trax.Apps.Services.KEngine.Handlers.Utils.ScriptsLoadingUtils import ProjectSpecificCalculationsLoader, \
    NUM_OF_RETRIES, TIMEOUT, retry, StorageFactory, S3_BUCKET, S3_REGION

__author__ = 'prasanna'


class LocalProjectSpecificCalculationsLoader(ProjectSpecificCalculationsLoader):

    def __init__(self):
        self._last_check_time = datetime.now()
        self._last_update_time = None
        self.load_projects_scripts_first_time()

    @retry(NUM_OF_RETRIES, Exception, timeout=TIMEOUT)
    def load_projects_scripts_first_time(self):
        storage_connector = StorageFactory.get_connector(mybucket=S3_BUCKET, region=S3_REGION)
        # self._update_scripts(storage_connector)
        return


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init("KEngine")
    # Monkey patch ProjectSpecificCalculationsLoader class.
    UnifiedHandler.ProjectSpecificCalculationsLoader = LocalProjectSpecificCalculationsLoader

    project_name = 'pngjp-sand2'
    sessions = [
        {"session_uid": "86A09858-6B90-454B-9547-8C33CD12688C",
         "scene_uid": "A83ADE17-2362-4B0A-861D-122DB168F8AA"
         },
        {"session_uid": "0FFF1A54-5759-4EA1-B260-4D604F7D7334",
         "scene_uid": "2B373278-8CF6-479F-9829-5B95A4D7930E"
         },
        {"session_uid": "86A09858-6B90-454B-9547-8C33CD12688C",
         "scene_uid": "64FF34E4-82E9-4860-8759-B65B4C55BFB6"
         },
        {"session_uid": "86A09858-6B90-454B-9547-8C33CD12688C",
         "scene_uid": "16DFB0C6-E86D-4B23-80DC-A3BFB6328CA1"
         }
    ]

    for session in sessions:
        session_uid = session['session_uid']
        scene_uid = session['scene_uid']

        message_scene = {
            "project_name": project_name,
            "event_name": "COLLECTION-TASK_PROCESSED",
            "timestamp": "2019-03-21T08:15:51.068040",
            "publisher_version": "RouterKEngine_v1.0.51",
            "queue_duration": 0,
            "wave_uid": "",
            "number_of_scenes": 1,
            "scenes": [
                scene_uid
            ],
            "session_uid": session_uid,
            "task_uid": "b595be77-f96a-4915-856f-f40cfc084329",
            "wave_type": "edit",
            "questionnaires": [],
            "message_type": "COLLECTION-TASK_PROCESSED",
            "persistence": {"flavor": None}
        }

        kenigineUnified = UnifiedHandler.KEngineUnifiedHandler()
        kenigineUnified._validate_message(message_scene)
        kenigineUnified._process_message(message_scene, None, MagicMock(), None)
