import os
import json
import pipes
import tempfile
import subprocess

def race2singularity(race_spec, config):
    global_state_mounts = [{
        'type': 'bind',
        'source': config['global_state_share'],
        'destination': config['global_state_share'],
        'readonly': False
    }]
    mounts = global_state_mounts
    quoted_string = ' '.join(map(pipes.quote,race_spec['argv']))
    mounts = ' '.join(
        [
            '-B {src}:{dst}'.format(src = m['source'], dst = m['destination'])
            for m in mounts
        ]
    )
    return 'singularity exec --no-home {mounts} --pwd /tmp --containall docker://{image} {quoted_string}'.format(
        workdir = race_spec['workdir'],
        image   = race_spec['image'],
        quoted_string = quoted_string,
        mounts = mounts
    )

class CondorBackend(object):
    status_dict = { #http://pages.cs.wisc.edu/~adesmet/status.html
        0:	'Unexpanded',
        1:	'Idle',
        2:	'Running',
        3:	'Removed',
        4:	'Completed',
        5:	'Held',
        6:	'Submission_err'
    }

    def __init__(self,global_state_share = '/afs', backend_share = None):
        self.global_state_share = global_state_share  # hacky for singularity
        self.backend_share = backend_share or os.path.abspath(os.path.join(os.curdir,'condor'))
        self.jobflavor = 'espresso'
        for subdir in ['log','error','output','scripts']:
            d = os.path.join(self.backend_share, subdir)
            if not os.path.exists(d):
                os.makedirs(d)
        
    def submit(self, race_spec):
        script = '''#!/bin/sh
set -e
echo "::: hello, running container :::"
{singularity_command}
echo "::: done :::"
echo "::: pwd :::"
ls -lrt
echo "::: bye :::"
    '''.format(singularity_command = race2singularity(race_spec, config = {'global_state_share': self.global_state_share}))

        print(script)

        runscript = tempfile.NamedTemporaryFile(dir = '{shr}/scripts'.format(shr = self.backend_share), delete = False)
        runscript.write(script)
        runscript.close()
        os.chmod(runscript.name,0o755)
        print(runscript.name)
        submit = '''
executable  = {runscript}
arguments   = $(ClusterID) $(ProcId)
output      = {backend_share}/output/pack.$(ClusterId).$(ProcId).out
error       = {backend_share}/error/pack.$(ClusterId).$(ProcId).err
log         = {backend_share}/log/pack.$(ClusterId).log
getenv      = True
+JobFlavour = "{flavor}"
should_transfer_files   = IF_NEEDED
when_to_transfer_output = ON_EXIT
queue 1
    '''.format(
            runscript = runscript.name,
            backend_share = self.backend_share,
            flavor = self.jobflavor

        )

        print('submitting\n{0}'.format(submit))

        out,err = subprocess.Popen(['condor_submit','-'], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE).communicate(submit)
        proxyfile = [l for l in out.split('\n') if 'UserLog' in l][0].split('=')[-1].replace('"','').strip()
        return {'proxyfile': proxyfile, 'scriptfile': runscript.name}

    def status(self, proxy):
        data = json.loads(subprocess.Popen(['condor_history','-json','-userlog',proxy['proxyfile']], stdout = subprocess.PIPE).communicate()[0])[0]
        return data

    def ready(self,resultproxy):
        return self.status_dict[self.status(resultproxy)['JobStatus']] == 'Completed'

    def successful(self,resultproxy):
        status = self.status(resultproxy)
        return self.status_dict[status['JobStatus']] == 'Completed' and status['ExitCode'] == 0

    def fail_info(self,resultproxy):
        return 'not sure why it failed'
