from typing import Set, List, Tuple
import asyncio
from flytekit.types.directory import FlyteDirectory
from io import BytesIO
from flytekit.types.file import FlyteFile
from flytekit import task, dynamic, LaunchPlan, workflow, Resources, map_task, conditional
from flytekit.experimental import eager
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
def paginate_directory(bucket_name: str, directory: str, internal: bool = True) -> Tuple[List[str], List[str]]:
    logger.debug(f"Paginating {directory}")
    minio_client = get_minio_client(internal=internal)
    objects = set()
    subpaths = set()
    path_objects = minio_client.list_objects(bucket_name, prefix=directory)
    for o in path_objects:
        if o.is_dir:
            subpaths.add(o.object_name)
        else:
            objects.add(o.object_name)
    logger.debug(f"{len(objects) + len(subpaths)} objects in directory")
    return (list(objects), list(subpaths))
    
@task(requests=Resources(cpu='1', mem='1Gi'))
def paginate_bucket(bucket_name: str, internal: bool = True) -> Tuple[List[str], List[str]]:
    logger.debug(f"Paginating bucket {bucket_name}")
    minio_client = get_minio_client(internal=internal)
    objects = set()
    directories = set()
    bucket_objects = minio_client.list_objects(bucket_name)
    for o in bucket_objects:
        if o.is_dir:
            directories.add(o.object_name)
        else:
            objects.add(o.object_name)
    logger.debug(f"{len(directories) + len(objects)} objects in bucket")
    return (list(directories), list(objects))
    


@eager
async def task_wise_paginate_through_filesystem(internal: bool = True) -> List[str]:
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
    logger.debug("Filling filesystem with data")
    await fill_filesystem(internal=internal)
    logger.debug("Done filling filesystem")
    object_paths = {}
    # this is defined in tests, there are other buckets that get used by 
    # flyte, so let's just use the one we create
    bucket_name = "big-bucket"
    logger.debug("going into paginate bucket")
    directories, objects = await paginate_bucket(internal=internal, bucket_name=bucket_name)
    logger.debug("Done paginating big bucket")
    for o in objects:
        object_paths[o] = True
    for directory in directories:
        subdirectories, new_objects = await paginate_directory(internal=internal, bucket_name=bucket_name, directory=directory)
        for o in new_objects:
            object_paths[o] = True
        while subdirectories:
            subdirectories, new_objects = await paginate_directory(internal=internal, bucket_name=bucket_name, directory=subdirectories)
            for o in new_objects:
                object_paths[o] = True
    return list(object_paths.keys())

@workflow
def dynamic_task_wise_paginate_through_filesystem(internal: bool = True) -> List[str]:
    logger.debug("Launching dynamic workflow for filesystem pagination")
    return task_wise_paginate_through_filesystem(internal=internal)

if __name__ == '__main__':
    asyncio.run(task_wise_paginate_through_filesystem(internal=False))