import json
from pathlib import Path
from typing import Any, Iterable
from abc import ABC, abstractmethod
from .drivers import Driver

INDENT = 4
SEP = '\n'

class SynchedList(ABC):
    """
    Abstract class of synched lists defining the workflow
    """
    
    def __init__(self, frequency : int = 5) -> None:
        super().__init__()
        
        assert isinstance(frequency, int)
        assert frequency > 0
        
        self.frequency = frequency
        self.lastflush = 0
        self.data = list()
        
    def __len__(self):
        return len(self.data)
        
    def append(self, item: Any) -> None:
        self.data.append(item)
        self.lastflush += 1
        self.autoflush()
        
    def extend(self, iterable: Iterable[Any]) -> None:
        # better than using len(iterable) in case iterable is a generator
        n = len(self.data)
        self.data.extend(iterable)
        self.lastflush += (len(self.data) - n)
        self.autoflush()
        
    def autoflush(self) -> None:
        if self.lastflush >= self.frequency:
            self.flush()
            
    @abstractmethod
    def flush(self) -> None:
        """
        Synchronize the data stored in memory to:
        - HDD: (Json, CSV, ...)
        - DataBase: (SQL, MongoDB ...)
        This depends on the media driver implemented
        """
        pass
    
    def __repr__(self) -> str:
        return str(self.data)   
    
    
class JsonSynchedList(SynchedList):
    """
    This a list wrapper that auto saves list content to a json in disk.
    This class does not support appending to jsons.
    Use DriverSynchedList with JsonDriver to benefit from the appending functionality
    
    filename: path to json where to save data
    frequency: frequency of the disk flush
    overwrite: overwrite existing file
    
    """
    
    def __init__(self, filename: str, frequency: int = 100, overwrite: bool = True) -> None:
        super().__init__(frequency=frequency)
        
        assert isinstance(overwrite, bool)
        assert isinstance(filename, (str, Path))
        
        self.filename = Path(filename)
        self.overwrite = overwrite
        
        if not self.overwrite and self.filename.is_file():
            raise FileExistsError('Cannot overwrite file')
        
        self.file = open(self.filename, "wb")

    def flush(self) -> None:
        """Flush data to disk"""
        self.file.seek(0)
        output = json.dumps(self.data, indent=4).encode()
        self.file.write(output)
        self.file.flush()
        self.lastflush = 0

    def __del__(self) -> None:
        """Auto flush to disk when object is removed"""
        if hasattr(self, 'file'):
            self.flush()
            self.file.close()
            

class DriverSynchedList(SynchedList):
    """ This a list wrapper that auto saves list content using a driver
    
    driver: driver to use to save data
    frequency: frequency of the disk flush
    """
    
    def __init__(self, driver: Driver, frequency: int = 100) -> None:
        super().__init__(frequency=frequency)
        
        assert issubclass(type(driver), Driver)
        self.driver = driver
            
    def flush(self) -> None:
        """Flush data to disk"""
        
        if self.lastflush != 0:
            self.driver.write(self.data[-self.lastflush:])
            self.lastflush = 0          
            
    def __del__(self) -> None:
        """Auto flush when object is removed"""
        
        if hasattr(self, 'driver'):
            self.flush()
            del self.driver      

