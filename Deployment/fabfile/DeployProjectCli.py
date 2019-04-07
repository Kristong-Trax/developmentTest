import argparse
import sys

from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from DeployProject import ProjectDeploy


def parse_arguments():
    parser = argparse.ArgumentParser(description='deploys projects ')
    parser.add_argument('--project', type=str, required=True, help='Project name to be deployed')
    parser.add_argument('-e', '--env', type=str, help='The environment - dev/int/prod')
    parser.add_argument('-am', default='local' , type=str, help='The environment - dev/int/prod')
    return parser.parse_args()


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('Deploy')
    args = parse_arguments()
    try:
        deploy_instance = ProjectDeploy(project=args.project)
        deploy_instance.deploy()

    except Exception as e:
        print e
        raise Exception("unable to deploy project ")
        # ProjectDeploy.send_mail(project, tag, e)


