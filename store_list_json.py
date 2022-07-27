from pathlib import Path
import json
from typing import Any, List, Tuple, Type, Union
import os

INDENT = 4
SEP = '\n'

def assert_type(object: Any, name: str, types: Union[Tuple[Type], Type]):
    
    if not isinstance(types, (tuple, list)):
        types = (types,)
    elif isinstance(types, list):
        types = tuple(types)
    
    typename = lambda x: x.__name__
    
    if not isinstance(object, types):
        raise TypeError(f"{name} must be of type \
                        {list(map(typename, types))} \
                        got type {typename(type(object))}")
    


class JsonDriver:
    def __init__(self, filename: str, overwrite: bool = False, append: bool = False) -> None:
        
        assert_type(filename, 'filename', (str, Path))
        assert_type(overwrite, 'overwrite', bool)
        assert_type(append, 'append', bool)
        
        self.filename = Path(filename)
        self.overwrite = overwrite
        self.append = append
        
        self.__post_init__()
            
    def __post_init__(self):
        """don't include start only when appending non empty file"""
        
        # File exists and we are not overwriting nor appending
        if Path(self.filename).is_file() and not (self.overwrite or self.append):
            raise FileExistsError(f'File already exists {self.filename}')
        
        self.isempty = True
        
        # Create new file if file does not exist or we overwriting
        if not Path(self.filename).is_file() or self.overwrite:
            open(self.filename, "w").close()
        
        # Open the file in read write update binary mode
        self.file = open(self.filename, mode='rb+')
        assert self.file is not None, f'Could not create file {self.filename}'
        
        if self.append and not self.overwrite:
            # Check if file is empty
            
            self.file.seek(0, os.SEEK_END)  # Go to end of file
            if self.file.tell() != 0:
                # if current position is truish (i.e != 0) then file is not empty       
                self.isempty = False
            else:
                # reset the cursor when file is empty
                self.file.seek(0)
        else:
            self.file.truncate()

    def write(self, data: List[dict]):
        
        if len(data) == 0: return

        output = json.dumps(data, indent=INDENT)
        
        # When file is empty we write the ouput directly
        # Else we have to trim the brackets for proper appending
        if not self.isempty:
            # Roll back the cursor to overwrite the last `\n]`
            END = -len(SEP)-len(']')
            self.file.seek(END, os.SEEK_END)
            
            # Format string to be appended
            START = len('[')+len(SEP)+INDENT
            output = f",{SEP}{' '*INDENT}" + output[START:]
        
        self.file.write(output.encode())
        self.isempty = False
    
    def __del__(self):
        self.file.close()

class StoreListJson():
    
    def __init__(self, filename: str, frequency: int = 100, overwrite: bool = True) -> None:
        
        """ This a list wrapper that auto saves list content to a json in disk
        
        filename: path to json where to save data
        frequency: frequency of the disk flush
        overwrite: overwrite existing file
        
        """
        assert isinstance(frequency, int)
        assert frequency > 0
        assert isinstance(overwrite, bool)
        assert isinstance(filename, (str, Path))
        
        self.filename = Path(filename)
        self.frequency = frequency
        self.overwrite = overwrite
        
        if not self.overwrite and self.filename.is_file():
            raise FileExistsError('Cannot overwrite file')
        
        self.data = []
        self.lastflush = 0
        self.file = open(self.filename, "wb")
        
        
    def push(self, element):
        """Add new element to list"""
        self.lastflush += 1
        self.data.append(element)
        
        if self.lastflush >= self.frequency:
            self.flush()
            
    def flush(self):
        """Flush data to disk"""
        self.file.seek(0)
        # json.dump(self.data, self.file, indent=4)
        output = json.dumps(self.data, indent=4).encode()
        self.file.write(output)
        self.lastflush = 0
        
        
    def __del__(self):
        """Auto flush to disk when object is removed"""
        if hasattr(self, 'file'):
            self.flush()
            self.file.close()
            
def test_json_driver():
    
    filename = 'tmp.json'
    
    data = []
    
    Path(filename).unlink(missing_ok=True)
    
    drv = JsonDriver(filename=filename, overwrite=True, append=False)
    for j in range(4):
        t = [{'index': i, 'data': i**2} for i in range(j*5,(j+1)*5)]
        data.extend(t)
        drv.write(t)    
    del drv
    
    drv = JsonDriver(filename=filename, overwrite=False, append=True)
    a = [{'index': i, 'data': i**2} for i in range(-3,0)]
    data.extend(a)
    drv.write(a)
    del drv
    
    
    drv = JsonDriver(filename=filename, overwrite=False, append=True)
    b = [{'index': i**3, 'data': i/5} for i in range(-50,-40)]
    data.extend(b)
    drv.write(b)
    del drv
    
    
    # saved_data = json.load(open(filename, mode='r'))
    # print(saved_data == data)
    # print(saved_data)
    # print(data)
    
    
def test_rb_ab():
    file = open('tt.json', mode='wb+')
    file.write('ABCDEFGHIJKLMONQRSTUVWXYZ'.encode())
    file.close()
    
    
    
    
    
    file = open('tt.json', mode='rb+')
    file.seek(-20, os.SEEK_END)
    
    file.truncate()
    
    
    file.write('123456789'.encode())
    file.close()
    
def test_storelist_json():
    l = StoreListJson('store.json', frequency=5, overwrite=True)
    
    for i in range(99):
        l.push({'index': i, 'data': i**2})
       
if __name__ == '__main__':
    
    test_json_driver()
    # test_rb_ab()
