import os
import sys
import json
from condor_json_api import CondorBackend

backend = CondorBackend()

def make_workdir_and_test_spec(workdir,message = 'Hello World'):
   if os.path.exists(workdir):
      raise RuntimeError('exists already choose a different one')
   os.makedirs(workdir)
   spec = {
      'image': 'lukasheinrich/busyboxwithafsdir',
      'argv': ['sh','-c','echo {message} > {workdir}/callbackdir.txt'.format(
         message = message,
         workdir = workdir
      )],
      'workdir': '/afs/cern.ch/work/l/lheinric/sing/workdir'
   }
   return spec

   proxy = backend.submit(spec)
   return proxy

if __name__ == '__main__':
   proxy = make_workdir_and_test_spec(sys.argv[1])
   print(json.dumps(proxy))
