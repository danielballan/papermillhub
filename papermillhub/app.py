import asyncio
import json
import logging
import os
import signal
from urllib.parse import urlparse, urlunparse

from jupyterhub.services.auth import HubAuthenticated
from tornado import web
from tornado.log import LogFormatter
from tornado.gen import IOLoop
from traitlets import Unicode, Bool, validate, default
from traitlets.config import Application, catch_config_error

from . import __version__ as VERSION
from .objects import DataManager
from .utils import TaskPool, get_ip


# Override default values for logging
Application.log_level.default_value = "INFO"
Application.log_format.default_value = (
    "%(color)s[%(levelname)1.1s %(asctime)s.%(msecs).03d "
    "%(name)s]%(end_color)s %(message)s"
)


class PapermillHub(Application):
    """A server for managing Papermill Jobs on JupyterHub"""

    name = "papermillhub"
    version = VERSION

    description = """Start a PapermillHub server"""

    examples = """

    Start the server with config file ``config.py``

        papermillhub --config config.py
    """

    aliases = {
        "log-level": "PapermillHub.log_level",
        "f": "PapermillHub.config_file",
        "config": "PapermillHub.config_file",
    }

    config_file = Unicode(
        "papermillhub_config.py", help="The config file to load", config=True
    )

    base_url = Unicode(
        help="The application's base URL",
        config=True,
    )

    @default("base_url")
    def _default_base_url(self):
        out = os.environ.get("JUPYTERHUB_SERVICE_URL", "http://:5000")
        print(out)
        return out

    @validate("base_url")
    def _normalize_base_url(self, proposal):
        url = proposal.value
        parsed = urlparse(url)
        if parsed.hostname in {"", "0.0.0.0"}:
            # Resolve local ip address
            host = get_ip()
            parsed = parsed._replace(netloc="%s:%i" % (host, parsed.port))
        # Ensure no trailing slash
        url = urlunparse(parsed._replace(path=parsed.path.rstrip("/")))
        return url

    db_url = Unicode(
        "sqlite:///:memory:",
        help="""
        The URL for the database. Default is in-memory only.

        If not in-memory, ``db_encrypt_keys`` must also be set.
        """,
        config=True,
    )

    db_debug = Bool(
        False, help="If True, all database operations will be logged", config=True
    )

    _log_formatter_cls = LogFormatter

    @catch_config_error
    def initialize(self, argv=None):
        super().initialize(argv)
        if self.subapp is not None:
            return
        self.load_config_file(self.config_file)
        self.init_logging()
        self.init_asyncio()
        self.init_database()
        self.init_tornado_application()

    def init_logging(self):
        # Prevent double log messages from tornado
        self.log.propagate = False

        # hook up tornado's loggers to our app handlers
        from tornado.log import app_log, access_log, gen_log

        for log in (app_log, access_log, gen_log):
            log.name = self.log.name
            log.handlers[:] = []
        logger = logging.getLogger("tornado")
        logger.handlers[:] = []
        logger.propagate = True
        logger.parent = self.log
        logger.setLevel(self.log.level)

    def init_asyncio(self):
        self.task_pool = TaskPool()

    def init_database(self):
        self.db = DataManager(url=self.db_url, echo=self.db_debug)

    def init_tornado_application(self):
        self.handlers = list(default_handlers)
        self.tornado_application = web.Application(
            self.handlers,
            log=self.log,
            papermill=self,
        )

    async def start_async(self):
        self.init_signal()
        self.db.load_database_state()

    async def start_or_exit(self):
        try:
            await self.start_async()
        except Exception:
            self.log.critical(
                "Failed to start papermillhub, shutting down",
                exc_info=True
            )
            await self.stop_async(stop_event_loop=False)
            self.exit(1)

    def start(self):
        if self.subapp is not None:
            return self.subapp.start()
        base_url = urlparse(self.base_url)
        self.http_server = self.tornado_application.listen(
            base_url.port, address=base_url.hostname
        )
        self.log.info("PapermillHub listening on %s", self.base_url)

        # Remaining setup is done asynchronously
        loop = IOLoop.current()
        loop.add_callback(self.start_or_exit)
        try:
            loop.start()
        except KeyboardInterrupt:
            print("\nInterrupted")

    def init_signal(self):
        loop = asyncio.get_event_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, self.handle_shutdown_signal, s)

    def handle_shutdown_signal(self, sig):
        self.log.info("Received signal %s, initiating shutdown...", sig.name)
        asyncio.ensure_future(self.stop_async())

    async def _stop_async(self, timeout=5):
        # Stop the server to prevent new requests
        if hasattr(self, "http_server"):
            self.http_server.stop()

        if hasattr(self, "task_pool"):
            await self.task_pool.close(timeout=timeout)

    async def stop_async(self, timeout=5, stop_event_loop=True):
        try:
            await self._stop_async(timeout=timeout)
        except Exception:
            self.log.error("Error while shutting down:", exc_info=True)
        # Stop the event loop
        if stop_event_loop:
            IOLoop.current().stop()


class APIHandler(HubAuthenticated, web.RequestHandler):
    @staticmethod
    def get_or_raise(data, key):
        try:
            return data[key]
        except KeyError:
            raise web.HTTPError(422, reason="%r parameter is required" % key)

    @property
    def json_data(self):
        if not hasattr(self, "_json_data"):
            content_type = self.request.headers.get("Content-Type", "")
            if content_type.startswith("application/json"):
                try:
                    self._json_data = json.loads(self.request.body)
                except Exception as exc:
                    raise web.HTTPError(422, reason=str(exc))
            else:
                raise web.HTTPError(422, reason="JSON request body required")
        return self._json_data

    def write_error(self, status_code, **kwargs):
        self.finish({"error": self._reason})

    @property
    def log(self):
        return self.settings.get("log")

    @property
    def papermill(self):
        return self.settings.get("papermill")

    def get_papermill_user(self):
        name = self.get_current_user()["name"]
        return self.papermill.db.get_or_create_user(name)


class JobsHandler(APIHandler):
    @web.authenticated
    async def post(self, job_id):
        if job_id:
            raise web.HTTPError(405)

        data = self.json_data
        in_path = self.get_or_raise(data, "in_path")
        out_path = self.get_or_raise(data, "out_path")
        parameters = data.get("parameters", {})
        kernel_name = data.get("kernel_name", "")

        job_id = await self.papermill.schedule(
            in_path,
            out_path,
            parameters=parameters,
            kernel_name=kernel_name
        )
        self.write({"job_id": job_id})

    @web.authenticated
    def get(self, job_id):
        user = self.get_papermill_user()

        if job_id:
            try:
                job = user.jobs[job_id]
            except KeyError:
                raise web.HTTPError(404, reason="Job %s does not exist" % job_id)
            result = job.model()
        else:
            jobs = user.active_jobs()
            result = {"jobs": [job.model() for job in jobs]}
        self.write(result)

    @web.authenticated
    async def delete(self, job_id):
        if not job_id:
            raise web.HTTPError(405)
        await self.papermill.cancel(job_id)
        self.set_status(204)


prefix = os.environ.get('JUPYTERHUB_SERVICE_PREFIX', '/')
default_handlers = [(prefix + "api/jobs/([a-zA-Z0-9-_.]*)", JobsHandler)]

main = PapermillHub.launch_instance
