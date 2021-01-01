import json				
import mmap
import sys
import threading
import time
from collections import OrderedDict

MAX_KEY_LEN = 32  # size of key
MAX_VALUE_SIZE = 16 * 1024  # 16 Kb
MAX_LOCAL_STORAGE_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB
LOCAL_STORAGE_PREPEND_PATH = "/tmp"  # default file path 

def is_legit(val, val_type="key"):
    if val_type == "key":
        if not isinstance(val, str):
            raise ValueError("Key must be of type string")
        return len(val) <= config.MAX_KEY_LEN
    elif val_type == "value":
        if isinstance(val, dict):
            return sys.getsizeof(val) <= config.MAX_VALUE_SIZE
        raise ValueError("Value must be of type dictionary")

class DataStoreVO:
    def __init__(self, value, created_at, ttl, *args, **kwargs):
        self.value = value
        self.ttl = ttl
        self.created_at = created_at
    def is_expired(self):
        if self.ttl is None:
            return False
        curr_ts = int(time.time() * 1000)
        return (curr_ts - self.created_at) > self.ttl * 1000

class DataStore:
    def __init__(self, file_descriptor, *args, **kwargs):
        self.__fd = file_descriptor
        self.__mmap = self._get_mmaped_fd()
        self.__data = OrderedDict()
        self.__lock = threading.Lock()
        self._read_data()
    def _get_mmaped_fd(self) -> mmap.mmap:
        try:
            mmaped_fd = mmap.mmap(self.__fd, 0, access=mmap.ACCESS_WRITE)
            mmaped_fd.resize(MAX_LOCAL_STORAGE_SIZE)
            return mmaped_fd
        except mmap.error:
            raise
    def _read_data(self) -> None:
        raw_data = self.__mmap[:].decode('ascii').rstrip('\0')
        self.__data = json.loads(raw_data)
    def create(self, key, value, ttl=None) -> None:
        with self.__lock:
            if key in self.__data:
                raise ValueError("Key already present.")
            if is_legit(key, val_type="key") and is_legit(value, val_type="value"):
                if ttl is not None:
                    try:
                        ttl = int(ttl)
                    except:
                        raise ValueError("Time-to-live must be an integer value.")
                value_arr = [value, int(time.time() * 1000), ttl]
                self.__data[key] = value_arr
                self.flush()
    def delete(self, key) -> None:
        with self.__lock:
            if key not in self.__data:
                return  
            del self.__data[key]
            self.flush()
    def get(self, key) -> dict:
        with self.__lock:
            if key not in self.__data:
                raise ValueError("Key not in datastore.")
            value = DataStoreVO(*self.__data.get(key))  
            if value.is_expired():
                self.__data.pop(key)
                self.flush()
                raise ValueError("Key expired.")
            return value.value
    def delete_all(self):
        with self.__lock:
            self.__data = dict()
            self.flush()
    def flush(self) -> None:
        self.__mmap.seek(0)
        data_string = bytes(json.dumps(self.__data).encode('ascii'))
        self.__mmap.write(data_string)
        empty_space_bytes = self.__mmap.size() - self.__mmap.tell()
        self.__mmap[self.__mmap.tell():] = b'\0' * empty_space_bytes
    def __getitem__(self, item):
        return self.get(item)
