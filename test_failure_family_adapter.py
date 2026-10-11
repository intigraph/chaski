import unittest
from failure_family_adapter import verdicts, validate_policy


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.policy={"default_threshold":5,"window_seconds":3600,"default_owner":"crew","family_thresholds":{},"tool_owners":{"Read":"reader"}}
        self.family="a"*64

    def report(self,count):
        return {"window_seconds":3600,"unknown":3,"excluded":2,"families":[{"id":self.family,"tool":"Read","current_window":count}]}

    def test_boundary_and_override(self):
        self.assertEqual(verdicts(self.report(4),self.policy)[0]["verdict"],"BELOW")
        above=verdicts(self.report(5),self.policy)[0]
        self.assertEqual(above["verdict"],"ABOVE")
        self.assertEqual(above["owner"],"reader")
        self.policy["family_thresholds"][self.family]=10
        self.assertEqual(verdicts(self.report(5),self.policy)[0]["verdict"],"BELOW")

    def test_unknown_policy_and_inventory(self):
        for key,value in (("default_owner","human"),("default_threshold",True),("window_seconds",0),("family_thresholds",{self.family:False})):
            with self.assertRaises(ValueError):validate_policy({**self.policy,key:value})
        for report in ({}, {**self.report(0),"unknown":None},self.report(-1)):
            with self.assertRaises(ValueError):verdicts(report,self.policy)
        duplicate=self.report(1);duplicate["families"]*=2
        with self.assertRaises(ValueError):verdicts(duplicate,self.policy)


if __name__ == "__main__": unittest.main()
