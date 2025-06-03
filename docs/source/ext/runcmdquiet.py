import csv
import os
import re
import shlex
import subprocess
import sys

from pathlib import Path

from docutils.parsers.rst import directives
from sphinx.directives import code

# This code is adapted from CWL User Guide, licensed under
# the CC BY 4.0 license, quoting their license:
#
# Attribution---You must give appropriate credit (mentioning
# that your work is derived from work that is Copyright ©
# the Common Workflow Language project, and, where practical,
# linking to https://www.commonwl.org/ ),...
# Ref: https://github.com/common-workflow-language/user_guide/blob/8abf537144d7b63c3561c1ff2b660543effd0eb0/LICENSE.md

""""
Patched version of https://github.com/sphinx-contrib/sphinxcontrib-runcmd
with default values to avoid having to re-type in every page. Also
prepends commands with a value (``$``), see https://github.com/invenia/sphinxcontrib-runcmd/issues/1.
Finally, it also checks if the command is ``cwltool``, and if then
tries to remove any paths from the command-line (not the logs).
"""

__version__ = "0.2.0"

# CONSTANTS
RE_SPLIT = re.compile(r"(?P<pattern>.*)(?<!\\)/(?P<replacement>.*)")


# These classes were in the .util module of the original directive.
class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Singleton(_Singleton("SingletonMeta", (object,), {})):
    pass


def run_command(command, working_directory):
    true_cmd = shlex.split(command)
    try:
        # The subprocess Popen function takes a ``cwd`` argument that
        # conveniently changes the working directory to run the command.
        #
        # We also patched the stderr to redirect to STDOUT,
        # so that stderr and stdout appear in order, as you would see in
        # a terminal.
        #
        # Finally, note that ``cwltool`` by default emits ANSI colors in the
        # terminal, which are harder to be parsed and/or rendered in Sphinx.
        # For that reason, we define --disable-color in the CWLTOOL_OPTIONS
        # environment variable, which is used by ``cwltool``.
        env = os.environ
        # cwl_options = set(env.get('CWLTOOL_OPTIONS', '').split(' '))
        # cwl_options.add('--disable-color')
        # env['CWLTOOL_OPTIONS'] = ' '.join(cwl_options)
        subp = subprocess.Popen(
            true_cmd,
            cwd=working_directory,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
    except Exception as e:
        out = ""
        err = e
    else:
        out, err = subp.communicate()
        encoding = sys.getfilesystemencoding()
        out = out.decode(encoding, "replace").rstrip()
        # The stderr is now combined with stdout.
        # err = err.decode(encoding, "replace").rstrip()

    if err and err != "":
        print("Error in runcmd: {}".format(err))
        out = "{}\n{}".format(out, err)

    return out


class RunCmdDirective(code.CodeBlock):
    has_content = False
    final_argument_whitespace = False
    required_arguments = 1
    optional_arguments = 99

    option_spec = {
        # code.CodeBlock option_spec
        "linenos": directives.flag,
        "dedent": int,
        "lineno-start": int,
        "emphasize-lines": directives.unchanged_required,
        "caption": directives.unchanged_required,
        "class": directives.class_option,
        "name": directives.unchanged,
        # RunCmdDirective option_spec
        "syntax": directives.unchanged,
        "replace": directives.unchanged,
        "prompt": directives.flag,
        "dedent-output": int,
        "working-directory": directives.unchanged
    }

    def run(self):
        syntax = self.options.get("syntax", "bash")

        self.arguments[0] = syntax
        # self.content = output
        node = super(RunCmdDirective, self).run()

        return node


def setup(app):
    app.add_directive("runcmdquiet", RunCmdDirective)

    return {"version": __version__}
