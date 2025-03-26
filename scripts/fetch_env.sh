#!/bin/bash
aws secretsmanager get-secret-value --secret-id datacollector/env --region eu-central-1 --query SecretString --output text > /data/datacollector/.env