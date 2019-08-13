import asyncio
import json
import sys

import nbformat
from nbconvert.preprocessors.execute import ExecutePreprocessor, CellExecutionError
from traitlets import Dict


__all__ = ("execute_notebook",)


class Translator(object):
    """Translate a dict of parameters to code"""
    @classmethod
    def translate(cls, val):
        if val is None:
            return cls.translate_none(val)
        elif isinstance(val, bool):
            return cls.translate_bool(val)
        elif isinstance(val, str):
            return cls.translate_str(val)
        elif isinstance(val, int):
            return cls.translate_int(val)
        elif isinstance(val, float):
            return cls.translate_float(val)
        elif isinstance(val, dict):
            return cls.translate_dict(val)
        elif isinstance(val, list):
            return cls.translate_list(val)
        return cls.translate_escaped_str(val)

    @classmethod
    def translate_none(cls, x):
        return repr(x)

    @classmethod
    def translate_int(cls, x):
        return repr(x)

    @classmethod
    def translate_float(cls, x):
        return repr(x)

    @classmethod
    def translate_str(cls, x):
        return repr(x)

    @classmethod
    def translate_bool(cls, x):
        return repr(x)

    @classmethod
    def translate_dict(cls, x):
        items = ", ".join(
            "%s: %s" % (cls.translate(k), cls.translate(v)) for k, v in x.items()
        )
        return "{%s}" % items

    @classmethod
    def translate_list(cls, x):
        items = ", ".join(cls.translate(v) for v in x)
        return "[%s]" % items

    @classmethod
    def translate_comment(cls, x):
        return "# %s" % x

    @classmethod
    def translate_assign(cls, name, val):
        return '{} = {}'.format(name, val)

    @classmethod
    def to_code(cls, parameters):
        lines = [cls.translate_comment('Parameters')]
        lines.extend(cls.translate_assign(k, cls.translate(v))
                     for k, v in parameters.items())
        return '\n'.join(lines)


def parameters_to_code(parameters):
    return Translator.to_code(parameters)


class PapermillHubPreprocessor(ExecutePreprocessor):
    papermill_parameters = Dict()

    def papermill_preprocess(self, nb):
        # Set the overall notebook metadata
        nb.metadata.setdefault("papermill", {})["parameters"] = self.papermill_parameters

        # If no parameters, nothing left to do
        if not self.papermill_parameters:
            return

        # Find the first "parameters" cell
        for idx, c in enumerate(nb.cells):
            if "parameters" in c.metadata.get("tags", ()):
                insert_at = idx + 1
                break
        else:
            insert_at = 0

        # Insert a new cell with the specified parameters
        param_content = parameters_to_code(self.papermill_parameters)
        newcell = nbformat.v4.new_code_cell(source=param_content)
        newcell.metadata['tags'] = ['injected-parameters']
        nb.cells.insert(insert_at, newcell)

    def preprocess(self, nb, resources=None, km=None):
        self.papermill_preprocess(nb)
        return super().preprocess(nb, resources=resources, km=km)


async def execute_notebook(nb, parameters=None, execution_timeout=None,
                           start_timeout=30, kernel_name=""):
    """Execute a notebook with a set of parameters

    Parameters
    ----------
    nb : nbformat.NotebookNode
        The notebook to execute.
    parameters : dict, optional
        Optional parameters to set before execution.
    execution_timeout : int, optional
        Per-cell execution timeout (in seconds). If not provided, default will
        be used.
    start_timeout : int, optional
        Timeout for starting the kernel. If not provided, default will be used.
    kernel_name : str, optional
        The kernel name to use. If not provided, the kernel specified in the
        notebook metadata will be used.

    Returns
    -------
    nb : nbformat.NotebookNode
        The executed notebook output.
    error : str or None
        If an exception occurred during execution, an error message is returned
        here. None otherwise.
    """
    kwargs = {"papermill_parameters": parameters or {},
              "timeout": execution_timeout,
              "startup_timeout": start_timeout,
              "kernel_name": kernel_name}

    stdin = json.dumps({"nb": json.dumps(nb), "kwargs": kwargs}).encode("utf8")

    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "papermillhub.executor",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate(stdin)
    except asyncio.CancelledError:
        # Stop the executor process if cancelled
        if proc is not None:
            proc.terminate()
            await proc.wait()
        raise

    stdout = stdout.decode("utf8", "replace")
    stderr = stderr.decode("utf8", "replace")

    if proc.returncode != 0:
        raise Exception(
            "Papermillhub job runner exited with return code %d:\n"
            "  stdout: %s\n"
            "  stderr: %s" % (proc.returncode, stdout, stderr)
        )
    result = json.loads(stdout)
    nb = result["nb"]
    internal_error = result["internal_error"]
    cell_error = result["cell_error"]

    if internal_error:
        raise Exception(internal_error)

    return nb, cell_error


def _runner():
    cell_error = internal_error = None
    try:
        message = json.load(sys.stdin)
        nb = nbformat.reads(message["nb"], as_version=4)
        kwargs = message["kwargs"]
        preprocessor = PapermillHubPreprocessor(**kwargs)
        preprocessor.preprocess(nb, resources={})
    except CellExecutionError as exc:
        cell_error = str(exc)
    except Exception as exc:
        internal_error = str(exc)

    json.dump(
        {"nb": nb, "cell_error": cell_error, "internal_error": internal_error},
        sys.stdout
    )
    sys.stdout.flush()


if __name__ == "__main__":
    _runner()
