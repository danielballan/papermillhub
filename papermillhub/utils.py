import asyncio
import socket
import weakref


class TaskPool(object):
    def __init__(self):
        self.pending_tasks = weakref.WeakSet()
        self.background_tasks = weakref.WeakSet()

    def create_task(self, task):
        out = asyncio.ensure_future(task)
        self.pending_tasks.add(out)
        return out

    def create_background_task(self, task):
        out = asyncio.ensure_future(task)
        self.background_tasks.add(out)
        return out

    async def close(self, timeout=5):
        # Cancel all background tasks
        for task in self.background_tasks:
            task.cancel()

        # Wait for a short period for any ongoing tasks to complete, before
        # canceling them
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.pending_tasks, return_exceptions=True), timeout
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

        # Now wait for all tasks to be actually completed
        try:
            await asyncio.gather(
                *self.pending_tasks, *self.background_tasks, return_exceptions=True
            )
        except asyncio.CancelledError:
            pass


def get_ip():
    try:
        # Try resolving by hostname first
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        pass

    # By using a UDP socket, we don't actually try to connect but
    # simply select the local address through which *host* is reachable.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # We use google's DNS server as a common public host to resolve the
        # address. Any ip could go here.
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except Exception:
        pass
    finally:
        sock.close()

    raise ValueError("Failed to determine local IP address")
