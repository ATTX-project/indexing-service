import click
import unittest
from esindex.app import init_api
from click.testing import CliRunner
from esindex.applib.messaging import ScalableRpcServer
from esindex.indexservice import INDEXService, number_of_workers, main, rpc
from mock import patch


class TestAPIStart(unittest.TestCase):
    """Test app is ok."""

    def setUp(self):
        """Set up test fixtures."""
        self.host = '127.0.0.1'
        self.workers = number_of_workers()
        self.port = 4304
        self.log = 'logs/server.log'
        options = {
            'bind': '{0}:{1}'.format(self.host, self.port),
            'workers': self.workers,
            'daemon': 'True',
            'errorlog': self.log
        }
        self.app = INDEXService(init_api(), options)
        # propagate the exceptions to the test client
        self.app.testing = True

    def tearDown(self):
        """Tear down test fixtures."""
        pass

    def test_command(self):
        """Test Running from command line."""
        @click.command()
        @click.option('--host')
        def start(host):
            click.echo('{0}'.format(host))

        runner = CliRunner()
        result = runner.invoke(start, input=self.host)
        assert not result.exception

    def running_app(self):
        """Test running app."""
        response = self.app.get('/')
        self.assertEqual(response.status_code, 404)

    @patch.object(ScalableRpcServer, 'start_server')
    def test_command_server(self, mock):
        """Test Running server from command line."""
        runner = CliRunner()
        result = runner.invoke(rpc)
        assert not result.exception

    @patch('esindex.indexservice.cli')
    def test_cli(self, mock):
        """Test if cli was called."""
        main()
        self.assertTrue(mock.called)


if __name__ == "__main__":
    unittest.main()
