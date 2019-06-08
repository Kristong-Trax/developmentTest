import re
import os
import shutil


__author__ = 'Nimrod'


class MoveCodeToINTEG:
    def __init__(self, src_project, dest_project):
        self.src_project = src_project.upper()
        self.dest_project = dest_project.upper()
        if self.src_project != 'DUMMY' and not self.validate_projects():
            print "Try using create_new_project.py instead!"
        self.projects_path = "{}/Projects/".format(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
        # checking for the source project's references and replacing them, while gathering all the class names
        self.change_new_project_files(dest_path, change_class_names=False)
        # changing all the classes' names
        self.change_new_project_files(dest_path, change_class_names=True)

    def change_new_project_files(self, path, change_class_names):
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
                self.change_new_project_files(
                    file_path + "/", change_class_names=change_class_names)
            else:
                if len(file_name) > 3 and file_name[-3:] == ".py":
                    with open(file_path, 'r') as content:
                        new_content, classes_names = self.change_file_references(content.read(),
                                                                                 change_class_names=change_class_names)
                        for class_name in classes_names:
                            self.classes_names[class_name] = self.dest_project + \
                                class_name.replace(self.src_project, "")
                    with open(file_path, 'w') as content:
                        content.write(new_content)

    def change_file_references(self, original_content, change_class_names):
        new_content = []
        classes_names = []
        for line in original_content.split("\n"):
            splitted_line = line.strip().split()
            if change_class_names:
                for class_name in self.classes_names.keys():
                    line = line.replace(class_name, self.classes_names[class_name])
            else:
                if splitted_line and splitted_line[0] == "class":  # finding declaration of classes
                    classes_names.append(splitted_line[1][:splitted_line[1].find("(")])
                project_name_appearances = re.findall(
                    "[\.\"\']{}[\.\"\']".format(self.src_project), line, re.IGNORECASE)
                for appearance in list(set(project_name_appearances)):
                    replacement = self.get_case_sensitive_replacement(appearance[1:-1])
                    line = line.replace(appearance, "{}{}{}".format(
                        appearance[0], replacement, appearance[-1]))
            new_content.append(line)
        new_content = "\n".join(new_content)
        return new_content, classes_names

    def get_case_sensitive_replacement(self, appearance):
        if appearance == self.src_project.upper():
            return self.dest_project.upper()
        elif appearance == self.src_project.lower():
            return self.dest_project.lower()
        else:
            return self.dest_project.capitalize()


if __name__ == '__main__':
    a = MoveCodeToINTEG("MARSRU2_SAND", "MARSRU_PROD")
    a.copy_project()
