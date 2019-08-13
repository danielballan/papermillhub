import json
import time
import uuid

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Unicode,
    ForeignKey,
    LargeBinary,
    TypeDecorator,
    create_engine,
    bindparam,
    select,
    event,
)
from sqlalchemy.pool import StaticPool

from .common import JobStatus


def timestamp():
    """An integer timestamp represented as milliseconds since the epoch UTC"""
    return int(time.time() * 1000)


class IntEnum(TypeDecorator):
    impl = Integer

    def __init__(self, enumclass, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._enumclass = enumclass

    def process_bind_param(self, value, dialect):
        return value.value

    def process_result_value(self, value, dialect):
        return self._enumclass(value)


class JSON(TypeDecorator):
    "Represents an immutable structure as a json-encoded string."

    impl = LargeBinary

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value).encode("utf-8")
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Unicode(255), nullable=False, unique=True)
)

jobs = Table(
    "jobs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Unicode(255), nullable=False, unique=True),
    Column(
        "user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    ),
    Column("in_path", Unicode(), nullable=False),
    Column("out_path", Unicode(), nullable=False),
    Column("parameters", JSON, nullable=False),
    Column("status", IntEnum(JobStatus), nullable=False),
    Column("spawner", Unicode(255), nullable=False),
    Column("start_time", Integer, nullable=False),
    Column("stop_time", Integer, nullable=True),
)


def register_foreign_keys(engine):
    """register PRAGMA foreign_keys=on on connection"""

    @event.listens_for(engine, "connect")
    def connect(dbapi_con, con_record):
        cursor = dbapi_con.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class DataManager(object):
    """Holds the internal state.

    Keeps the memory representation in-sync with the database.
    """

    def __init__(self, url="sqlite:///:memory:", **kwargs):
        if url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}

        if url in ("sqlite://", "sqlite:///:memory:"):
            kwargs["poolclass"] = StaticPool

        engine = create_engine(url, **kwargs)
        if url.startswith("sqlite"):
            register_foreign_keys(engine)

        metadata.create_all(engine)

        self.db = engine
        self.username_to_user = {}
        self.id_to_job = {}

    def load_database_state(self):
        # Load all existing users into memory
        id_to_user = {}
        for u in self.db.execute(users.select()):
            user = User(id=u.id, name=u.name)
            self.username_to_user[user.name] = user
            id_to_user[user.id] = user

        # Next load all existing jobs into memory
        for j in self.db.execute(jobs.select()):
            user = id_to_user[j.user_id]
            job = Job(
                id=j.id,
                name=j.name,
                user=user,
                parameters=j.parameters,
                status=j.status,
                start_time=j.start_time,
                stop_time=j.stop_time,
            )
            self.id_to_job[j.id] = job
            user.jobs[job.name] = job

    def cleanup_expired(self, max_age_in_seconds):
        cutoff = timestamp() - max_age_in_seconds * 1000
        with self.db.begin() as conn:
            to_delete = conn.execute(
                select([jobs.c.id]).where(jobs.c.stop_time < cutoff)
            ).fetchall()

            if to_delete:
                to_delete = [i for i, in to_delete]

                conn.execute(
                    jobs.delete().where(jobs.c.id == bindparam("id")),
                    [{"id": i} for i in to_delete],
                )

                for i in to_delete:
                    job = self.id_to_job.pop(i)
                    del job.user.jobs[job.name]

        return len(to_delete)

    def get_or_create_user(self, username):
        """Lookup a user if they exist, otherwise create a new user"""
        user = self.username_to_user.get(username)
        if user is None:
            res = self.db.execute(users.insert().values(name=username))
            user = User(id=res.inserted_primary_key[0], name=username)
            self.username_to_user[username] = user
        return user

    def create_job(self, user, in_path, out_path, parameters):
        """Create a new job for a user"""
        common = {
            "name": uuid.uuid4().hex,
            "in_path": in_path,
            "out_path": out_path,
            "parameters": parameters,
            "spawner": "",
            "status": JobStatus.PENDING,
            "start_time": timestamp(),
        }

        with self.db.begin() as conn:
            res = conn.execute(
                jobs.insert().values(
                    user_id=user.id,
                    **common,
                )
            )
            job = Job(
                id=res.inserted_primary_key[0],
                user=user,
                **common,
            )
            self.id_to_job[job.id] = job
            user.jobs[job.name] = job

        return job

    def update_job(self, job, **kwargs):
        """Update a job's state"""
        with self.db.begin() as conn:
            conn.execute(
                jobs.update().where(jobs.c.id == job.id).values(**kwargs)
            )
            for k, v in kwargs.items():
                setattr(job, k, v)


class User(object):
    def __init__(self, id=None, name=None):
        self.id = id
        self.name = name
        self.jobs = {}

    def active_jobs(self):
        return [j for j in self.jobs.values() if j.is_active()]


class Job(object):
    def __init__(
        self,
        id=None,
        name=None,
        user=None,
        in_path=None,
        out_path=None,
        parameters=None,
        status=None,
        spawner="",
        start_time=None,
        stop_time=None,
    ):
        self.id = id
        self.name = name
        self.user = user
        self.in_path = in_path
        self.out_path = out_path
        self.parameters = parameters
        self.status = status
        self.spawner = spawner
        self.start_time = start_time
        self.stop_time = stop_time

    def is_active(self):
        return self.status <= JobStatus.RUNNING

    def model(self):
        return {"id": self.name,
                "in_path": self.in_path,
                "out_path": self.out_path,
                "parameters": self.parameters,
                "status": self.status.name(),
                "start_time": self.start_time,
                "stop_time": self.stop_time}
