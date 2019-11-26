import getpass
import os
import shutil
import time
import traceback
from datetime import datetime
from Trax.Utils.Conf.Configuration import Config

from Trax.Cloud.Services.Connector.Keys import EmailUsers
from Trax.Cloud.Services.Mailers.Factory import MailerFactory
from Trax.Cloud.Services.Storage.Factory import StorageFactory
from git import Actor

from Deployment.fabfile.Git import create_new_tag, get_last_tag_on_current_commit, SSH_CMD, get_sdk_factory_repository
from Deployment.fabfile.UtilsDeployment import DeploymentUtils

KPI_UTILS_V2 = 'KPIUtils_v2'
KPI_UTILS = 'KPIUtils'
PROJECT_FOLDER = 'Projects'
OUT_OF_THE_BOX = 'OutOfTheBox'

ROLLBACK_SUFFIX = '_rollback'
TRAX_ACE_LIVE = 'trax_ace_live'
STORAGE_BUCKET = 'trax-k-engine-scripts'
STORAGE_PATH = 'trax_ace_live_latest'
TAR_FILE_NAME = 'latest.tar.gz'
CLOUDS = ['AWS', 'GCP']
ENVIRONMENTS = ['prod', 'int']


class ProjectDeployment(object):

    @staticmethod
    def delete_pyc_files(root_dir):
        print "Removing .pyc files on {}".format(root_dir)
        print os.system("find {path} -type f -name '*.pyc'".format(path=root_dir))
        os.system("find {path} -type f -name '*.pyc' -delete".format(path=root_dir))

    @staticmethod
    def ignore_files(files):
        return [file_name for file_name in files if '.pyc' in file_name]

    @staticmethod
    def build_paths_and_perform_overwrite(live_git_folder, folder_to_copy):
        src_path = '{}/'.format(get_sdk_factory_repository()) + folder_to_copy
        dst_path = '{}/'.format(live_git_folder) + folder_to_copy

        print "overwrite from {} to {}".format(src_path, dst_path)
        DeploymentUtils.recursive_overwrite(src_path, dst_path, ignore=ProjectDeployment.ignore_files)

    @staticmethod
    def make_tarfile(source_dir):
        files_and_dirs_to_copy = [PROJECT_FOLDER, KPI_UTILS, KPI_UTILS_V2, OUT_OF_THE_BOX]
        if Config.get_environment().lower() == 'prod':
            files_and_dirs_to_copy.append('version.txt')
        return DeploymentUtils.make_tar_file_for_files(source_dir, files_and_dirs_to_copy)

    @staticmethod
    def upload_to_live_git(live_git_folder, live_repo, project=None, project_tag=None, tag_pregfix='trax_ace_live'):
        print "upload_to_live_git START"
        author = Actor(getpass.getuser(), '')
        now = datetime.utcnow()
        now.replace(microsecond=0)
        if project is not None:
            version_details = '''tag: {}\n'project: {}\n'author: {}\n'time: {}'''.format(project_tag, project, author, now)
            project_path = PROJECT_FOLDER + '/{}'.format(project)
            ProjectDeployment.update_version_file(os.path.join(live_git_folder, project_path), project_tag)
        else:
            version_details = '''author: {}\n'time: {}'''.format(author, now)
        try:
            with live_repo.git.custom_environment(GIT_SSH_COMMAND=SSH_CMD):
                live_repo.git.add(A=True)
                print "performing commit '{}'".format(version_details)
                live_repo.index.commit(version_details)
                print "Creating new tag"
                live_tag = create_new_tag(tag_pregfix, live_repo)
                live_repo.create_tag(path=live_tag, message=version_details)
                print "pushing tag: {}".format(live_tag)
                live_repo.remotes.origin.push('master', tags=True)
                print "upload_to_live_git FINISHED"

        except Exception as e:
            print 'upload_to_live_git Exception {}'.format(e)
            traceback.print_exc()
            live_repo.git.reset('--hard', 'origin/master')
            raise e

    @staticmethod
    def update_version_file(path, tag):
        version_file_path = os.path.join(path, 'version.txt')
        with open(version_file_path, 'w+') as f:
            f.write('tag:{}'.format(tag))

    @staticmethod
    def push_factory_new_tag(project, repo):
        print "push_new_tag START"
        # Filter relevant tags
        current_tag = get_last_tag_on_current_commit(repo, project)
        if current_tag:
            return current_tag

        new_tag = create_new_tag(project, repo)

        try:
            with repo.git.custom_environment(GIT_SSH_COMMAND=SSH_CMD):
                author = Actor(getpass.getuser(), '')
                repo.create_tag(new_tag, message='Author: {0}, data: {1}'.format(author, datetime.now()))
                print "pushing new tag: {}".format(new_tag)
                repo.remotes.origin.push('origin/master', tags=True)

        except Exception as e:
            print 'push_new_tag Exception {}'.format(e)
            traceback.print_exc()
            raise

        print "push_new_tag FINISHED"
        return new_tag

    @staticmethod
    def copy_to_storage_server(live_folder, live_repo, update_version=True):
        # live_folder, live_repo = get_live_repository()
        try:
            if Config.get_environment().lower() == 'prod':
                tag = get_last_tag_on_current_commit(live_repo, TRAX_ACE_LIVE)
                if not tag:
                    raise Exception('LiveTagNotExistsOnCurrentCommit')
                if update_version:
                    ProjectDeployment.update_version_file(live_folder, tag)
            tar_file_stream = ProjectDeployment.make_tarfile(live_folder)
            for cloud in CLOUDS:
                print 'bucket name', STORAGE_BUCKET, 'cloud to store', cloud
                storage_connector = StorageFactory.get_connector(mybucket=STORAGE_BUCKET, region='us-east-1',
                                                                 cloud=cloud)
                env = Config.get_environment().lower()
                if env == 'prod':
                    s3_envs = ENVIRONMENTS
                else:
                    s3_envs = [env]

                for s3_env in s3_envs:
                    storage_folder_name = ProjectDeployment.get_trax_ace_live_folder(s3_env)
                    print "Uploading file to Remove folder-> {}".format(storage_folder_name)
                    DeploymentUtils.save_file_stream(storage_connector, storage_folder_name, TAR_FILE_NAME, tar_file_stream)
        except Exception as e:
            print e
            raise
        finally:
            shutil.rmtree(live_folder)

    @staticmethod
    def get_trax_ace_live_folder(env):
        return STORAGE_PATH + '_{}'.format(env)

    @staticmethod
    def send_mail(project, tag):
        mailing_list = ['ps_sw_team@Trax-Tech.com']

        deployment_summary = {'Project': project,
                              'Date': time.strftime("%Y-%m-%d"),
                              'User': getpass.getuser(),
                              'Language': 'Python',
                              'Tag': tag
                              }
        mailer = MailerFactory.get_mailer(EmailUsers.TraxMailer)

        mail_body = ProjectDeployment.summary_table('', deployment_summary, fab_result=True)
        mailer_subject = 'Trax ACE Factory Deployment on {project}'.format(project=project)
        mailer.send_email(mailing_list, mail_body, mailer_subject)

    @staticmethod
    def summary_table(email_body, deployment_summary, fab_result=None):
        if deployment_summary:
            bgcolor = '8FBC8F'
            if not fab_result:
                bgcolor = 'DC143C'

            summary = '<BR>Summary:<BR>'
            summary += '<TABLE border=3 width="50%" bgcolor=9FB8C6 style="color:Black;" >'

            for key in deployment_summary:
                summary += '<TR bgcolor="' + bgcolor + '">'
                summary += '<TD>' + str(key).replace('_', ' ') + '</TD>'
                summary += '<TD><B>' + str(deployment_summary[key]) + '</B></TD>'
                summary += '</TR>'

            summary += '</TABLE>'

            signature = '<BR>'
            signature += 'Sincerely yours, <BR>'
            signature += 'THE WALL BREAKERS TEAM <BR>'
            email_body += summary + signature

        return email_body
