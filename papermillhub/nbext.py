import asyncio
import atexit
import json
import uuid

from notebook.utils import url_path_join
from notebook.base.handlers import APIHandler
from notebook.services.contents.manager import ContentsManager
from tornado import web
from traitlets import Instance, Dict
from traitlets.config import LoggingConfigurable

from .common import JobStatus
from .executor import execute_notebook


class PapermillJob(object):
    def __init__(self, id, in_path, out_path, parameters=None, kernel_name=""):
        self.id = id
        self.in_path = in_path
        self.out_path = out_path
        self.parameters = parameters
        self.kernel_name = kernel_name
        # Runtime state
        self.status = JobStatus.PENDING
        self.task = None
        self.error = None

    def model(self):
        return {"id": self.id,
                "in_path": self.in_path,
                "out_path": self.out_path,
                "parameters": self.parameters,
                "kernel_name": self.kernel_name,
                "status": self.status.name,
                "error": self.error}


class PapermillRunner(LoggingConfigurable):
    jobs = Dict()
    contents_manager = Instance(ContentsManager)

    @property
    def queue(self):
        try:
            return self._queue
        except AttributeError:
            self._queue = asyncio.Queue()
            self._queue_handler_task = asyncio.ensure_future(self.queue_handler())
            return self._queue

    async def _stop(self):
        try:
            self.log.info("Stopping papermillhub job runner!")
            self._queue_handler_task.cancel()
            await self._queue_handler_task
        except Exception:
            pass

    def stop(self):
        if hasattr(self, "_queue_handler_task") and not self._queue_handler_task.done():
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(self._stop(), loop)
            else:
                loop.run_until_complete(self._stop())

    async def schedule(self, in_path, out_path, parameters=None, kernel_name=""):
        job = PapermillJob(
            id=uuid.uuid4().hex,
            in_path=in_path,
            out_path=out_path,
            parameters=parameters,
            kernel_name=kernel_name
        )
        self.jobs[job.id] = job
        await self.queue.put(job)
        self.log.info("Papermill job %s queued for notebook %r", job.id, in_path)
        return job.id

    def cancel(self, job_id):
        job = self.jobs.get(job_id)

        if job is None or job.status >= JobStatus.SUCCEEDED:
            return

        if job.status == JobStatus.RUNNING:
            self.log.info("Stopping running papermill job %s", job.id)
            job.task.cancel()
        else:
            self.log.info("Cancelling pending papermill job %s", job.id)

        job.status = JobStatus.CANCELLED

    def get_job(self, job_id):
        return self.jobs[job_id]

    def active_jobs(self):
        return [job for job in self.jobs.values() if job.status <= JobStatus.RUNNING]

    async def run_job(self, job):
        self.log.info("Starting papermill job %s", job.id)

        self.log.info("Reading notebook from %s for job %s", job.in_path, job.id)
        try:
            contents_model = self.contents_manager.get(job.in_path)
            nb_in = contents_model["content"]
        except Exception as exc:
            job.status = JobStatus.ERRORED
            job.error = "Error while reading notebook:\n\n%s" % exc
            self.log.warning(job.error)
            return

        self.log.info("Executing notebook for job %s", job.id)
        try:
            nb_out, error = await execute_notebook(
                nb_in,
                parameters=job.parameters,
                kernel_name=job.kernel_name
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            job.status = JobStatus.ERRORED
            job.error = "Error while executing notebook:\n\n%s" % exc
            self.log.warning(job.error)
            return

        self.log.info("Writing notebook to %s for job %s", job.out_path, job.id)
        try:
            contents_model = contents_model.copy()
            contents_model["content"] = nb_out
            self.contents_manager.save(contents_model, job.out_path)
        except Exception as exc:
            job.status = JobStatus.ERRORED
            job.error = "Error while writing notebook:\n\n%s" % exc
            self.log.warning(job.error)
            return

        if error:
            job.status = JobStatus.FAILED
            job.error = error
            self.log.info(
                "Papermill job %s failed, see written output for more information",
                job.id
            )
        else:
            job.status = JobStatus.SUCCEEDED
            self.log.info("Papermill job %s completed successfully", job.id)

    async def queue_handler(self):
        while True:
            # Wait for a new task
            job = await self.queue.get()

            # If the task was already cancelled move on
            if job.status == JobStatus.CANCELLED:
                continue

            # Run the task until completion or cancellation
            job.status = JobStatus.RUNNING
            job.task = asyncio.ensure_future(self.run_job(job))
            try:
                await job.task
            except asyncio.CancelledError:
                if job.status != JobStatus.CANCELLED:
                    raise


class PapermillhubHandler(APIHandler):
    @staticmethod
    def get_or_raise(data, key):
        try:
            return data[key]
        except KeyError:
            raise web.HTTPError(422, reason="%r parameter is required" % key)

    def initialize(self, papermill_runner):
        self.papermill_runner = papermill_runner

    @web.authenticated
    async def post(self, job_id):
        if job_id:
            raise web.HTTPError(405)

        try:
            data = json.loads(self.request.body)
        except Exception:
            raise web.HTTPError(422)

        in_path = self.get_or_raise(data, "in_path")
        out_path = self.get_or_raise(data, "out_path")
        parameters = data.get("parameters", {})
        kernel_name = data.get("kernel_name", "")

        job_id = await self.papermill_runner.schedule(
            in_path,
            out_path,
            parameters=parameters,
            kernel_name=kernel_name
        )
        self.write({"job_id": job_id})

    @web.authenticated
    def get(self, job_id):
        if job_id:
            try:
                job = self.papermill_runner.get_job(job_id)
            except KeyError:
                raise web.HTTPError(404, reason="Job %s does not exist" % job_id)
            result = job.model()
        else:
            jobs = self.papermill_runner.active_jobs()
            result = {"jobs": [job.model() for job in jobs]}
        self.write(result)

    @web.authenticated
    def delete(self, job_id):
        if not job_id:
            raise web.HTTPError(405)
        self.papermill_runner.cancel(job_id)
        self.set_status(204)


def load_jupyter_server_extension(nb_server_app):
    """Setup the papermillhub server extension."""
    web_app = nb_server_app.web_app
    base_url = web_app.settings["base_url"]

    papermill_runner = PapermillRunner(
        contents_manager=nb_server_app.contents_manager,
        log=nb_server_app.log,
    )
    # Cleanup background process on exit
    atexit.register(papermill_runner.stop)

    web_app.add_handlers(".*$", [
        (url_path_join(base_url, "/papermillhub/([a-zA-Z0-9-_.]*)"),
         PapermillhubHandler,
         {"papermill_runner": papermill_runner})
    ])
