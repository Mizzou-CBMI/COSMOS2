import random
import string
import os
import tempfile

from django.test import TestCase
from cosmos.models import JobManager
from cosmos.models import JobStatusError


def slow(f):
    def decorated(self):
        f(self) #comment to skip slow functions
        pass 
    return decorated



tmpdir = tempfile.gettempdir()
test_script = os.path.join(os.getcwd(),'JobManager/test/test_script.py')

class Test_JobManager(TestCase):
    def setUp(self):
        self.JM = JobManager.objects.__create()
        self.JM.init_session()
        self.JM.save()
        
    def tearDown(self):
        self.JM.close_session()
        
    def test_addJob(self):
        job = self.JM.addJobAttempt(command_script_path="/bin/echo hi", jobName="test_addJob", output_dir=tmpdir)
        jobs = self.JM.getJobs()
        assert len(jobs) == 1
        assert job.jobName == "test_addJob"
    
    def test_submitJob(self):
        job = self.JM.addJobAttempt(command_script_path="/bin/echo hi", jobName="test_submitJob", output_dir=tmpdir)
        self.JM.submitJob(job)
        jobs = self.JM.getJobs()
        assert len(jobs) == 1
        self.assertRaises(JobStatusError,self.JM.submitJob,job)
    
    def _checkJobOutput_of_TestScript(self,job,job_input):
        pass
    
    @slow
    def test_JobReturnsCorrectResults(self):
        """Test that a submitted job returns the correct stdout and stderr"""
        #test_script Prints the first argument to stdout, and the reverse to stderr
        random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(600)) #note cmdArgs limited to 1000 chars
        job = self.JM.addJobAttempt(command_script_path="%s %s" % (test_script,random_string), jobName="test_waitForJob", output_dir=tmpdir)
        self.JM.submitJob(job)
        self.JM.waitForJob(job)
        assert job.stdout_head == random_string[:500] #first 500 chars
        assert job.stderr_head == random_string[::-1][:500] #last 500 chars in reverse
    
#    @slow
#    def test_waitForAnyJob(self):
#        job1 = self.JM.addJobAttempt(command="/bin/echo hi", jobName="test_waitForAnyJob1", output_dir=tmpdir)
#        job2 = self.JM.addJobAttempt(command="/bin/echo hi", jobName="test_waitForAnyJob2", output_dir=tmpdir)
#        self.JM.submitJob(job1)
#        self.JM.submitJob(job2)
#        rjob1 = self.JM.waitForAnyJob()
#        rjob2 = self.JM.waitForAnyJob()
#        assert rjob1 == job1 or rjob1 == job2 #make sure we're returning one of the two jobs
#        assert rjob2 == job1 or rjob2 == job2 #make sure we're returning one of the two jobs
#        assert self.JM.waitForAnyJob() == None #out of jobs, make sure fxn returning None
#        assert self.JM.waitForAnyJob() == None
    