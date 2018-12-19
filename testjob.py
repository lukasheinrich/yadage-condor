from condor_json_api import CondorBackend

backend = CondorBackend()
spec = {
   'image': 'lukasheinrich/busyboxwithafsdir',
   'argv': ['sh','-c','echo hello needit > /afs/cern.ch/work/l/lheinric/sing/workdir/callbackdir.txt'],
   'workdir': '/afs/cern.ch/work/l/lheinric/sing/workdir'
}

proxy = backend.submit(spec)
print(proxy)
