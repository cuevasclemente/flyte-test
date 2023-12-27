from typing import Set, List, Tuple
from flytekit.types.directory import FlyteDirectory
from io import BytesIO
from flytekit.types.file import FlyteFile
from flytekit import task, dynamic, LaunchPlan, workflow, Resources, map_task, conditional
from minio import Minio
import logging

def configure_logger():
    logging.basicConfig(
        format='%(filename)s: %(asctime)s - %(levelname)s - %(message)s -  Line:%(lineno)d',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG
    )
configure_logger()
logger = logging.getLogger(__name__)


def get_minio_client(internal: bool = True):
    """Make a minio client
    if internal is True, use the internal address of the minio host accessbile within the flyte sandbox
    otherwise use the localhost endpoint accesssible on the sandbox hosting machine"""
    if internal:
        host = "flyte-sandbox-minio.flyte:9000"
    else:
        host = "localhost:30002"
    client = Minio(host, access_key="minio", secret_key="miniostorage", secure=False)
    return client

@task
def fill_filesystem(internal: bool = True):
    minio_client = get_minio_client(internal=internal)
    logger.debug("minio_client obtained for filling filesystem")
    if minio_client.bucket_exists("big-bucket"):
        logger.debug("bucket 'big-bucket' already exists")
    else:
        minio_client.make_bucket("big-bucket")
        logger.debug("creating bucket 'big-bucket'")
    prefix = "0"
    logger.debug("Filling 'big-bucket' with many many files and directories")
    for i in range(1000):
        if i%100 == 0:
            prefix = str(i)
        minio_client.put_object(bucket_name="big-bucket", object_name=f"{prefix}/file_{i}", data=BytesIO(bytes(i)), length=len(bytes(i)))
    logger.debug("All done filling 'big-bucket'")

    
    



@task(requests=Resources(cpu='2', mem='1Gi'))
def paginate_bucket(bucket_name: str) -> (list[str], list[str]):
    logging.debug(f"Paginating bucket {bucket_name}")
    minio_client = get_minio_client()
    objects = set()
    directories = set()
    bucket_objects = minio_client.list_objects(bucket_name)
    for o in bucket_objects:
        if o.is_dir:
            directories.add(o.object_name)
        else:
            objects.add(o.object_name)
    logging.debug(f"{len(directories) + len(objects)} objects in bucket")
    logging.debug(f"Finished paginating bucket {bucket_name}")
    new_objects = list(objects)
    new_directories = list(directories)
    return (new_directories, new_objects)

@task(requests=Resources(cpu='2', mem='1Gi'))
def paginate_directory(bucket_name: str, directory: str, internal: bool = True) -> (list[str], list[str]):
    logging.debug(f"Paginating directory {directory}")
    minio_client = get_minio_client(internal=internal)
    objects = set()
    subpaths = set()
    path_objects = minio_client.list_objects(bucket_name, prefix=directory)
    logging.debug(f"{len(path_objects)} objects in directory")
    for o in path_objects:
        if o.is_dir():
            subpaths.add(o.object_name)
        else:
            objects.add(o.object_name)
    logging.debug(f"Finished paginating directory {directory}")
    return (list(subpaths), list(objects))

    


@dynamic
def task_wise_paginate_through_filesystem(internal: bool = True) -> list[str]:
    """A dynamic workflow:
    Using the minio_client, get a list of all bucket names
    For each bucket:
            get the name of each object in the bucket AND get all the prefixes in the bucket
            For each object in the bucket:
                add that object's path to our enumerated set of object paths
            A: For each prefix in the bucket:
                Get all objects and prefixes in the prefix
                For all objects in the prefix:
                    Add eac object's path to our enumerated set of object paths
                For all the prefixes in the prefix:
                    run from A 

    Returns a set of every object in the bucket without recursively exploring the bucket in a single step
    """
    logging.debug("Filling filesystem with data")
    fill_filesystem(internal=internal)
    logging.debug("Done filling filesystem")
    object_paths = set()
    # this is defined in tests, there are other buckets that get used by 
    # flyte, so let's just use the one we create
    bucket_name = "big-bucket"
    logger.debug("going into paginate bucket")
    directories, objects = paginate_bucket(internal=internal, bucket_name=bucket_name)
    for o in objects:
        object_paths.add(o)
    for directory in directories:
        subdirectories, objects = paginate_directory(bucket_name=bucket_name, directory=directory, internal=internal)
        object_paths.update(set(objects))
        while subdirectories:
            subdirectories, objects =paginate_directory(bucket_name=bucket_name, directory=directory, internal=internal)
            for o in objects:
                object_paths.add(o)
    logging.debug("All object paths listed")
    return list(set(object_paths))

@workflow
def dynamic_task_wise_paginate_through_filesystem(internal: bool = True) -> List[str]:
    logger.debug("Launching dynamic workflow for filesystem pagination")
    return task_wise_paginate_through_filesystem(internal=internal)

if __name__ == '__main__':
    standard_scale_launch_plan = LaunchPlan.get_or_create(
        dynamic_task_wise_paginate_through_filesystem,
        name="paginate_through_fs",
        default_inputs={"internal": False}
    )
    standard_scale_launch_plan(internal=False)