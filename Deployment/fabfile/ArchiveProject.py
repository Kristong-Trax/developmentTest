# import getpass
# import os
# import shutil
# import sys
# import traceback
# from datetime import datetime
#
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config
# from git import Actor
# from Deployment.fabfile.BaseDeployment import ProjectDeployment, KPI_UTILS, KPI_UTILS_V2, PROJECT_FOLDER
# from Deployment.fabfile.Git import create_new_tag, get_live_repository, get_live_git_folder, get_kpi_factory_repo, \
#     SSH_CMD, get_sdk_factory_repository, copy_to_ace_live
#
#
# class ProjectArchive(ProjectDeployment):
#
#     @staticmethod
#     def archive_project(projects):
#         try:
#             # Create the mail object
#             print 'Archive process have STARTED for the following projects={}'.format(','.join(projects))
#             print 'Fetching the KPI_FACTORY Repository'
#
#             kpi_factory_folder, repo = get_kpi_factory_repo()
#             print 'KPI_FACTORY Repository Successfully'
#             projects_folder = os.path.join(kpi_factory_folder, 'Projects')
#             archive_folder = os.path.join(kpi_factory_folder, 'Archive')
#             author = Actor(getpass.getuser(), '')
#             now = datetime.utcnow()
#             now.replace(microsecond=0)
#             version_details = '''Archiving\n'author: {}\n'time: {}'''.format(author, now)
#             moved_projects = 0
#             for project in projects:
#                 converted_project_name = project.replace('-', '_').upper()
#                 project_project_path = os.path.join(projects_folder, converted_project_name)
#                 archive_project_path = os.path.join(archive_folder, converted_project_name)
#                 if os.path.exists(project_project_path) and os.path.isdir(project_project_path):
#                     if not os.path.exists(archive_project_path):
#                         print '****Archiving project: {}****'.format(converted_project_name)
#                         shutil.move(project_project_path, archive_project_path)
#                         moved_projects += 1
#                     else:
#                         print 'WARNING: The destination folder for project {} aleady exists. ' \
#                               'This project cannot be archived'.format(converted_project_name)
#                 else:
#                     'WARNING: The request project {} does not exist under the /Projects folder' \
#                         .format(converted_project_name)
#
#             if moved_projects > 0:
#                 try:
#                     print 'Performing updates in the remote KPI_FACTORY repository.'
#                     with repo.git.custom_environment(GIT_SSH_COMMAND=SSH_CMD):
#                         repo.git.add(A=True)
#                         print "***Performing commit '{}'".format(version_details)
#                         repo.index.commit(version_details)
#                         print "***Creating new tag"
#                         live_tag = create_new_tag('archive_test', repo)
#                         repo.create_tag(path=live_tag, message=version_details)
#                         print "***Pushing tag: {}".format(live_tag)
#                         repo.remotes.origin.push('master', tags=True)
#                         print "KPI_FACTORY repository update have FINISHED"
#
#                         print 'Deleting {}'.format(kpi_factory_folder)
#                         shutil.rmtree(kpi_factory_folder)
#
#                         ProjectArchive.deploy(version_details)
#                     print 'Archive process FINISHED SUCCESSFULLY tag={}'.format(live_tag)
#                 except Exception as e:
#                     print 'KPI_FACTORY Exception {}'.format(e)
#                     traceback.print_exc()
#                     repo.git.reset('--hard', 'origin/master')
#                     raise e
#                 # send_mail(project, tag)
#             else:
#                 print 'WARNING: No valid project was found to me archived'
#         except Exception as e:
#             print e
#             # send_mail(project, tag, e)
#             sys.exit(1)
#
#     @staticmethod
#     def deploy(version_details):
#         try:
#             # Create the mail object
#             print 'Staring deployment of the new filtered Structure'
#             factory_git_folder, repo = get_sdk_factory_repository()
#             # root_path = os.path.join(factory_git_folder)
#             copy_to_ace_live()
#
#             # project_directory = os.path.join(factory_git_folder, 'Projects')
#             # projects = os.listdir(project_directory)
#             # projects.sort()
#             # for project in projects:
#             #     converted_project_name = project.replace('-', '_').upper()
#             #     root_path = os.path.join(factory_git_folder, 'Projects', converted_project_name)
#             #     if os.path.exists(root_path) and os.path.isdir(root_path):
#             #         print "root path={}".format(root_path)
#             #         modules_checkup(root_path)
#
#             print "***Creating new tag"
#             live_tag = create_new_tag('archive_test', repo)
#             repo.create_tag(path=live_tag, message=version_details)
#             print "***Pushing new tag"
#             tag = ProjectArchive.push_factory_new_tag('test', repo)
#             print "***Uploading to Git Live"
#             ProjectArchive.upload_to_live_git(version_details)
#
#             if not os.path.exists(factory_git_folder):
#                 print 'deleting {}'.format(factory_git_folder)
#                 shutil.rmtree(factory_git_folder)
#             # send_mail(project, tag)
#             print 'deploy FINISHED SUCCESSFULLY {} tag={}'.format(version_details, tag)
#         except Exception as e:
#             print e
#             # send_mail(project, tag, e)
#             sys.exit(1)
#
#     @staticmethod
#     def upload_to_live_git(version_details):
#
#         print "upload_to_live_git START"
#         live_git_folder, live_repo = get_live_repository()
#
#         ProjectArchive.build_paths_and_perform_overwrite(live_git_folder, PROJECT_FOLDER)
#         ProjectArchive.build_paths_and_perform_overwrite(live_git_folder, KPI_UTILS)
#         ProjectArchive.build_paths_and_perform_overwrite(live_git_folder, KPI_UTILS_V2)
#
#         try:
#             with live_repo.git.custom_environment(GIT_SSH_COMMAND=SSH_CMD):
#                 live_repo.git.add(A=True)
#                 print "performing commit '{}'".format(version_details)
#                 live_repo.index.commit(version_details)
#                 print "Creating new tag"
#                 live_tag = create_new_tag('archive_test', live_repo)
#                 live_repo.create_tag(path=live_tag, message=version_details)
#                 print "pushing tag: {}".format(live_tag)
#                 live_repo.remotes.origin.push('master', tags=True)
#                 print "upload_to_live_git FINISHED"
#
#         except Exception as e:
#             print 'upload_to_live_git Exception {}'.format(e)
#             traceback.print_exc()
#             live_repo.git.reset('--hard', 'origin/master')
#             raise e
#         finally:
#             if not os.path.exists(get_live_git_folder()):
#                 print 'deleting {}'.format(get_live_git_folder())
#                 shutil.rmtree(get_live_git_folder())
#
#
# if __name__ == '__main__':
#     Config.init()
#     LoggerInitializer.init('Project Archive')
#     projects = ['integ28']
#     ProjectArchive.archive_project(projects=projects)
#     ProjectArchive.copy_to_storage_server()
