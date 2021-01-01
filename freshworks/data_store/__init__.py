import fcntl
import os
from .datastore import DataStore


MAX_KEY_LEN = 32  # size of key
MAX_VALUE_SIZE = 16 * 1024  # 16 Kb
MAX_LOCAL_STORAGE_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB
LOCAL_STORAGE_PREPEND_PATH = "/tmp"  # default file path 


def get_file_name() -> str:
    import uuid
    uniq_append_string = uuid.uuid4().hex
    return "LOCAL_STORAGE_{}".format(uniq_append_string)
def get_instance(file_name=None) -> DataStore:
    if file_name is None:
        file_name = get_file_name()
    full_file_name = f"{LOCAL_STORAGE_PREPEND_PATH}/{file_name}"
    file_descriptor = os.open(full_file_name, os.O_CREAT | os.O_RDWR)
    try:
        print("Acquiring file lock ")
        fcntl.flock(file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise BlockingIOError("Resource is already locked")
    except Exception:
        raise
    else:
        print("File lock acquired")
    if not os.path.isfile(full_file_name) or os.fstat(file_descriptor).st_size == 0:
        with open(full_file_name, 'ab') as f:
            string = "{}"
            f.write(bytes(string.encode('ascii')))
    return DataStore(file_descriptor)

__all__ = ['get_instance']
