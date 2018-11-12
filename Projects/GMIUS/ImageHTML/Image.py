import cv2
import pandas as pd
from matplotlib import pyplot as plt
from boto.s3.key import Key
from Projects.GMIUS.ImageHTML.HTML_BASE import HTML_Base
from boto.compat import BytesIO, six, urllib, encodebytes
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Storage.Factory import StorageFactory

import SimpleHTTPServer
import SocketServer

class local_server():
    def __init__(self):
        pass

class ImageMaker:
    def __init__(self, project, scene, bucket='traxus', keyword='Stitched_Scene', ext='jpeg',
                 display_attribs=['product_name', 'product_type', 'category', 'manufacturer_name',
                                  'brand_name', 'product_fk', 'scene_match_fk', 'bay_number',
                                  'shelf_number', 'facing_sequence_number'], additional_attribs=[]):
        self.bucket = bucket
        self.project = project
        self.scene = scene
        self.display_attribs = display_attribs + additional_attribs
        self.s3_path = '{}/scene-images/{}/'.format(self.project, self.scene)
        self.down_path = '/tmp/{}/{}.ext'.format(self.project, self.scene)
        self.keyword = keyword
        self.amz_conn = None
        self.connect_amz()
        self.html = None
        self.image = None

        self.download_stitched_image_html()
        self.html_builder = HTML_Base(self.html, self.display_attribs)

    def connect_amz(self):
        try:
            Config.init()
            self.amz_conn = StorageFactory.get_connector(self.bucket)
        except Exception as e:
            pass
            # Log.error(self.log_prefix + " Failed to retrieve with error: " + str(e))

    def find_most_recent_relevant_file(self):
        files = [file for file in self.amz_conn.bucket.list(self.s3_path) if self.keyword in file.key]
        return sorted(files, key=lambda x: x.last_modified)[-1].key


    def download_stitched_image_html(self):
        file = self.find_most_recent_relevant_file()
        fp = BytesIO()
        self.amz_conn.download_file(file, fp)
        self.html = fp.getvalue().replace('\n', '').replace('\t', '')



# a = cluster_visualizer('rinielsenus', 342186)
# z = a.html_builder.return_html()
# print('complete')