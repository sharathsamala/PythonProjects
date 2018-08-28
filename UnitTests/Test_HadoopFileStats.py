#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Sharath Samala'

# Library and external module declaration
import unittest
import os, sys

from HadoopStats import HadoopFileStats
from mock import patch, MagicMock

class Test_HadoopFileStats(unittest.TestCase):
    """

    """

    def setUp(self):
        pass


    #@patch('awscli.clidriver.CLIDriver.main', MagicMock(return_value=0))
    #@patch('MoveCopy.MoveCopy._check_if_file_or_folder',
           #MagicMock(return_value=True))
    def test_tc01_valid_params(self):

        hadoopFS = HadoopFileStats()

        self.assertEqual("xyz", "xyz")


suite = unittest.TestLoader().loadTestsFromTestCase(hadoopFS)
unittest.TextTestRunner(verbosity=2).run(suite)
