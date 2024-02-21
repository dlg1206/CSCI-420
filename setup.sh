#!/bin/bash
# File: setup.sh
# Description: Setup venv and install packages for windows
#
# @author: Derek Garcia

# Create virtual environment
py -m venv venv

# Start env
source venv/Scripts/activate

# install packages
pip install -r requirements.txt