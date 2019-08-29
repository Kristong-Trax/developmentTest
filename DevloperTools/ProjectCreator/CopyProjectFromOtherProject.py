from DevloperTools.ProjectCreator.Consts import get_project_name_and_directory_name
import re
import os
import shutil

__author__ = 'Nimrod'


class CopyCodeToNewProject:
    def __init__(self, src_project, dest_project):
        self.src_project_name, self.src_project = get_project_name_and_directory_name(src_project)
        self.dest_project_name, self.dest_project = get_project_name_and_directory_name(dest_project)
        if self.src_project != 'DUMMY' and not self.validate_projects():
            print "Try using CreateNewProject.py instead!"
        self.projects_path = "{}/Projects/".format(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        self.classes_names = {}

    def validate_projects(self):
        if 'INTEG' in self.src_project or 'INTEG' in self.dest_project:
            return True
        elif self.src_project.split('_')[0] == self.dest_project.split('_')[0]:
            return True
        elif self.src_project.split('_')[0][:-2] == self.dest_project.split('_')[0][:-2]:
            return True
        else:
            return False

    def copy_project(self):
        src_path = self.projects_path + self.src_project + "/"
        dest_path = self.projects_path + self.dest_project + "/"
        if os.path.exists(dest_path):
            shutil.rmtree(dest_path)
        shutil.copytree(src_path, dest_path)
        self.change_new_project_files(dest_path)

    def change_new_project_files(self, path):
        if os.path.isdir(path):
            files = os.listdir(path)
        else:
            if path[-1] == '/':
                path = path[:-1]
            files = [path.split('/')[-1]]
            path = '/'.join(path.split('/')[:-1]) + '/'
        for file_name in files:
            file_path = path + file_name
            if os.path.isdir(file_path):
                self.change_new_project_files(file_path + "/")
            else:
                if len(file_name) > 3 and file_name[-3:] == ".py":
                    with open(file_path, 'r') as content:
                        new_content = self.change_file_references(content.read())
                    with open(file_path, 'w') as content:
                        content.write(new_content)
            new_file_path = path + self.change_file_references(file_name)
            os.rename(file_path, new_file_path)

    def change_file_references(self, original_content):
        new_content = original_content.replace(self.src_project, self.dest_project).replace(
            self.src_project_name, self.dest_project_name)
        return new_content


if __name__ == '__main__':
    source_project_name = "avi-sand"
    dest_project_name = "avisss"
    a = CopyCodeToNewProject(source_project_name, dest_project_name)
    a.copy_project()
