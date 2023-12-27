# workflows

A template for the recommended layout of a Flyte enabled repository for code written in python using [flytekit](https://docs.flyte.org/projects/flytekit/en/latest/).

## Usage

To get up and running with your Flyte project, we recommend following the
[Flyte getting started guide](https://docs.flyte.org/en/latest/getting_started.html).

We recommend using a git repository to version this project, so that you can
use the git sha to version your Flyte workflows.

## How to recreate my testing environment
Create and source a virtual environment (I'm using python 3.11.7).

Install requirements in the virtual environment.

Use the makefile to `make bootstrap` after you have installed requirements from requirements.txt and for flyte.

Either:
    run `python workflows/test_dynamic_workflow.py`
    OR
    run `pyflyte register --image localhost:30000/workflows:0.1 workflows` (or `make register`), 
       then run the `workflows.test_dynamic_workflow.dynamic_task_wise_paginate_through_filesystem` workflow from Flyte 
