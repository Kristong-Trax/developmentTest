import os
import sys

from KPIUtils_v2.Utils.Decorators.Decorators import log_task
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Deployment.fabfile.BaseDeployment import ProjectDeployment
from Deployment.fabfile.Git import copy_to_ace_live, get_kpi_factory_repository, \
    get_sdk_factory_repository, get_live_repository
from Deployment.fabfile.ProjectValidation import ProjectValidator


class ProjectDeploy(ProjectDeployment):
    def __init__(self, project=None):
        self.project = project

    @log_task(action='ProjectDeployment', message='Deploying Project', environment='prod')
    def deploy(self):
        sdk_factory_git_folder, sdk_repo = get_sdk_factory_repository()
        ace_live_git_folder, ace_live_repo = get_live_repository()
        if self.project is not None:
            try:
                # Create the mail object
                print 'deploy START project={}'.format(self.project)
                # get the local tmp sdk_factory dir and remote git object
                kpi_factory_git_folder, kpi_repo = get_kpi_factory_repository()
                converted_project_name = self.project.replace('-', '_').upper()
                root_path = os.path.join(ace_live_git_folder, 'Projects', converted_project_name)
                print "root path={}".format(root_path)
                copy_to_ace_live(sdk_factory_git_folder, ace_live_git_folder, kpi_factory_git_folder,
                                 converted_project_name)
                ProjectDeployment.delete_pyc_files(ace_live_git_folder)
                ProjectValidator.modules_checkup(root_path)
                kpi_tag = ProjectDeploy.push_factory_new_tag(converted_project_name, kpi_repo)
                ProjectDeploy.upload_to_live_git(ace_live_git_folder, ace_live_repo,
                                                 converted_project_name, project_tag=kpi_tag)
                ProjectDeploy.copy_to_storage_server(ace_live_git_folder, ace_live_repo)
                ProjectDeploy.send_mail(self.project, kpi_tag)
                print 'deploy FINISHED SUCCESSFULLY {} tag={}'.format(converted_project_name, kpi_tag)
            except Exception as e:
                print e
                # ProjectDeploy.send_mail(project, tag, e)
                sys.exit(1)
        else:
            try:
                print 'deploy START on sdk_factory'
                copy_to_ace_live(sdk_factory_git_folder, ace_live_git_folder)
                ProjectDeploy.upload_to_live_git(ace_live_git_folder, ace_live_repo)
                ProjectDeploy.copy_to_storage_server(ace_live_git_folder, ace_live_repo)
                print 'deploy FINISHED SUCCESSFULLY - sdk factory'
            except Exception as e:
                print e
                # ProjectDeploy.send_mail(project, tag, e)
                sys.exit(1)
        sys.exit(0)


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('Deploy')
    deploy_instance = ProjectDeploy(project='batru')
    deploy_instance.deploy()
    pass
