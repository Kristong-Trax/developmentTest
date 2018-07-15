import argparse

from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from DeployProject import ProjectDeploy


def parse_arguments():
    parser = argparse.ArgumentParser(description='deploys projects ')
    parser.add_argument('--project', type=str, required=True, help='Project name to be deployed')
    return parser.parse_args()


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('Deploy')
    args = parse_arguments()
    deploy_instance = ProjectDeploy(project=args.project)
    deploy_instance.deploy()
    pass