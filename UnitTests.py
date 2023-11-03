import unittest
from DWFA import *

class TestDWFA(unittest.TestCase):

    def test_add_job(self):
        job = Job(1, 0, 1)
        self.assertEqual(job.id, 1)
        self.assertEqual(job.arrival_time, 0)
        self.assertEqual(job.service_time, 1)

    def test_start_job(self):
        server = Server(1, 1)
        job = Job(1, 0, 1)
        server.start_job(job)
        self.assertTrue(server.busy)
        self.assertEqual(server.job, job)
        self.assertEqual(server.number_of_jobs, 1)
    
    def test_finish_job(self):
        server = Server(1, 1)
        job = Job(1, 0, 1)
        server.start_job(job)
        server.finish_job()
        self.assertFalse(server.busy)
        self.assertEqual(server.expected_finish_time, 0)

    def test_invalid_num_servers(self):
        with self.assertRaises(ValueError):
            DWFA(0, [], 1)
    
    def test_invalid_jobs_list(self):
        with self.assertRaises(ValueError):
            DWFA(1, [], 1)

    def test_virtual_time(self):
        job = Job(1, 0, 1)
        server = Server(1, 1)
        server.start_job(job)
        server.finish_job()
        self.assertEqual(server.virtual_time, 1)

    def test_add_job_to_queue(self):
        job = Job(1, 0, 1)
        server = Server(1, 1)
        server.start_job(job)
        server.finish_job()
        self.assertEqual(server.virtual_time, 1)
        self.assertEqual(server.expected_finish_time, 0)
        
if __name__ == '__main__':
    unittest.main()