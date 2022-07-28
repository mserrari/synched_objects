import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable

from drivers import Driver, JsonDriver

INDENT = 4
SEP = '\n'
TESTING_FOLDER = 'tmp'


class SynchedList(ABC):
    
    def __init__(self, frequency : int = 5) -> None:
        super().__init__()
        
        assert isinstance(frequency, int)
        assert frequency > 0
        
        self.frequency = frequency
        self.lastflush = 0
        self.data = list()
        
    def __len__(self):
        return len(self.data)
        
    def append(self, item: Any) -> int:
        self.data.append(item)
        self.lastflush += 1
        self.autoflush()
        
    def extend(self, iterable: Iterable[Any]):
        # better than using len(iterable) in case iterable is a generator
        n = len(self.data)
        self.data.extend(iterable)
        self.lastflush += (len(self.data) - n)
        self.autoflush()
        
    def autoflush(self):
        if self.lastflush >= self.frequency:
            self.flush()
            
    @abstractmethod
    def flush(self):
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
    This a list wrapper that auto saves list content to a json in disk
    
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

    def flush(self):
        """Flush data to disk"""
        self.file.seek(0)
        output = json.dumps(self.data, indent=4).encode()
        self.file.write(output)
        self.file.flush()
        self.lastflush = 0

    def __del__(self):
        """Auto flush to disk when object is removed"""
        if hasattr(self, 'file'):
            self.flush()
            self.file.close()
            

class DriverSynchedList(SynchedList):
    def __init__(self, driver: Driver, frequency: int = 100) -> None:
        
        """ This a list wrapper that auto saves list content using a driver ()
        
        driver: driver to use to save data
        frequency: frequency of the disk flush
        """
        super().__init__(frequency=frequency)
        
        assert issubclass(type(driver), Driver)
        self.driver = driver
            
    def flush(self):
        """Flush data to disk"""
        
        if self.lastflush != 0:
            self.driver.write(self.data[-self.lastflush:])
            self.lastflush = 0          
            
    def __del__(self):
        """Auto flush when object is removed"""
        
        if hasattr(self, 'driver'):
            self.flush()
            del self.driver      


def data_generator(total=10):
    data = [{'index': i, 'data': i**2} for i in range(total)]
    i = 0
    while i < len(data):
        
        if i % 2 == 0 or i == len(data) - 1:
            yield data[i], 'append'
            i += 1
        else:
            yield data[i:i+3], 'extend'
            i += 3
            
def fill_with_data(l: SynchedList, total=20):
    data_gen = data_generator(total=total)
    output = list()
    
    for d, mode in data_gen:
        if mode == 'append':
            l.append(d)
            output.append(d)
        else:
            l.extend(d)
            output.extend(d)
            
    return output
    
def test_jsonsynchedlist():
    
    file = Path(TESTING_FOLDER) / 'json_synched_list.json'
    l = JsonSynchedList(file,
                        frequency=5,
                        overwrite=True)
    
    data = fill_with_data(l)
    del l # flush
    
    with open(file, 'rb') as f:
        loaded_data = json.load(f)

    print(data == loaded_data)

    

        
def test_driversynchedlist_jsondriver(total=20, frequency=5):
    
    """
    Testing append and extend of synched list using the json driver
    """
    
    file = Path(TESTING_FOLDER) / 'driver_synched_list.json'
    driver = JsonDriver(file, overwrite=True, append=True)
    l = DriverSynchedList(driver, frequency=frequency)

    data = fill_with_data(l)
    del l
    
    with open(file, 'rb') as f:
        loaded_data = json.load(f)

    print(data == loaded_data)
    


if __name__ == '__main__':
    
    Path(f'{TESTING_FOLDER}').mkdir(exist_ok=True)
    
    test_jsonsynchedlist()
    test_driversynchedlist_jsondriver()
    test_driversynchedlist_jsondriver(total=20, frequency=5)
    test_driversynchedlist_jsondriver(total=20, frequency=5)
    test_driversynchedlist_jsondriver(total=21, frequency=5)
    test_driversynchedlist_jsondriver(total=21, frequency=30)
    test_driversynchedlist_jsondriver(total=230, frequency=11)
