#!/usr/bin/env python3
"""CDK entrypoint:  cdk deploy BillingDev --context stage=dev"""
import aws_cdk as cdk
from billing_stack import BillingStack

app = cdk.App()
stage = app.node.try_get_context("stage") or "dev"
BillingStack(app, f"Billing{stage.capitalize()}", stage=stage,
             env=cdk.Environment(region="us-west-2"))
app.synth()
