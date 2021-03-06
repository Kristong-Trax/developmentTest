from datetime import datetime
from Trax.Apps.Services.KEngine.Handlers import UnifiedHandler
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from mock import MagicMock
from Trax.Apps.Services.KEngine.Handlers.Utils.ScriptsLoadingUtils import ProjectSpecificCalculationsLoader, \
    NUM_OF_RETRIES, TIMEOUT, retry, StorageFactory, S3_BUCKET, S3_REGION
import pandas as pd

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
         "scene_uid": "B52A863F-83BE-4B50-9FF4-FB89A84E6BDD"
         },
        # {"session_uid": "95EB81AE-D7D5-4E40-87EB-771D504AAEB9",
        #  "scene_uid": "5F002B6D-BE99-41CD-81B0-887A88D50DD0"
        #  },
        # {"session_uid": "E2C4A7B5-AF2B-40BA-9D35-932FC1826568",
        #  "scene_uid": "3C77CC9B-C41B-4382-AD12-C503B3C75724"
        #  },
        # {"session_uid": "E2C4A7B5-AF2B-40BA-9D35-932FC1826568",
        #  "scene_uid": "A9B92E66-85F9-492E-BAA2-CC5495A1F364"
        #  },
        # {"session_uid": "86A09858-6B90-454B-9547-8C33CD12688C",
        #  "scene_uid": "16DFB0C6-E86D-4B23-80DC-A3BFB6328CA1"
        #  }
    ]

    for session in sessions:
        session_uid = session['session_uid']
        scene_uid = session['scene_uid']
        print(session_uid, scene_uid)

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

        try:
            kenigineUnified = UnifiedHandler.KEngineUnifiedHandler()
            kenigineUnified._validate_message(message_scene)
            kenigineUnified._process_message(message_scene, None, MagicMock(), None)
        except Exception as e:
            print ("Error {}".format(e))
