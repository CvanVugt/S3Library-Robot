'''
@author: Coen van Vugt & Tinie Sluijter

Copyright 2018
'''

from keywords import Keywords

__version__ = '0.1'

class S3Robot(Keywords):
    """
    S3Robot provides keywords for interacting with
    Amazon Web Services Simple Storage Service (S3) object storage. This library uses the boto3- and botocore library (make sure to install those via pip > "pip install boto3" and "pip install botocore").
    """
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
    ROBOT_LIBRARY_VERSION = __version__