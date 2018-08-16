# Standard library imports
import datetime as dt
import os
import subprocess
import signal
import time

# 3rd party library imports
import yaml


class RunLoadTest(object):

    def __init__(self, configfile):

        with open(configfile, mode='rt') as f:
            self.config = yaml.load(f)

    def run(self):
        for idx, interval in enumerate(self.config['intervals']):
            print(f"{idx:02d}:  {dt.datetime.now()}")

            planfile = f"plan_{idx:02d}.jmx"

            # This assumes, of course, that jmeter is on your path.
            command = f"jmeter -n -t {planfile}"

            # See https://stackoverflow.com/questions/4789837
            # /how-to-terminate-a-python-subprocess-launched-with-shell-true
            p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE,
                                 preexec_fn=os.setsid)
            time.sleep(self.config['intervals'][idx] * 60)

            # Ok, we are done with this load level.  Kill jmeter.  Kill it real
            # good.
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)

            # Save the output.  The stdout output is the JMeter output that
            # we really want.  There usually should not be any stderr output.
            stdout, stderr = p.communicate()

            filename = f"stdout_{idx:02d}.txt"
            with open(filename, mode='wb') as f:
                f.write(stdout)

            if stderr is None:
                continue
            filename = f"stderr_{idx:02d}.txt"
            with open(filename, mode='wb') as f:
                f.write(stderr)
