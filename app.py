#!/usr/bin/env python3
import os
from aws_cdk import core

from my_reality.my_reality_stack import MyRealityStack



#try:
#   AWS_ACCOUNT_ID = os.environ['AWS_ACCOUNT_ID']
#except KeyError as error:
#    raise KeyError(
#        'Please set "AWS_ACCOUNT_ID" environment variable with the AWS '
#        'account ID where the CDK Stack is supposed to be deployed.'
#    ) from error

env = core.Environment( ## not required but using it for future - multiple acc, different regions 
    account=131202687297,
    region='eu-central-1',)

app = core.App()
# My stack 
my_stack_1 = MyRealityStack(app, "my-reality",env=env)




app.synth()
