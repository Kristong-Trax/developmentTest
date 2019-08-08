
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock


__author__ = 'idanr'


class TestNESTLEIL(TestCase):

    @mock.patch('Projects.NESTLEIL.Utils.KPIToolBox.ProjectConnector')
    def set_up(self):
        super(TestNESTLEIL, self).set_up()
