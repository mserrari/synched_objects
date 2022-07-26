from msilib.schema import File
from pathlib import Path
import json
from typing import Any, List, Tuple, Type, Union
import os

INDENT = 4
SEP = '\n'
DTYPE = ']'

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
    

def get_mode(filename: Union[str, Path], overwrite: bool = False, append: bool = False):
    """
    Three cases:
    
    1. overwrite is True: Create new file with mode 'wb'
    2. overwrite if False and append is True: Append to existing file mode 'rb+'
    3. overwrite if False and append is False: Raise an error if there is an existing file, mode 'wb'
    
    """
    
    mode = 'wb' # overwrite file wether it exists or not
    if not overwrite and Path(filename).is_file():
        if not append:
            raise FileExistsError(f'File already exists {filename}')
        
        mode = 'rb+'
        
    return mode

class JsonDriver:
    def __init__(self, filename: str, overwrite: bool = False, append: bool = False) -> None:
        
        assert_type(filename, 'filename', (str, Path))
        assert_type(overwrite, 'overwrite', bool)
        assert_type(append, 'append', [bool])
        
        self.filename = Path(filename)
        self.overwrite = overwrite
        self.append = append
        
        self.__post_init__()
            
    def __post_init__(self):
        """don't include start only when appending non empty file"""
        
        self.mode = get_mode(self.filename, overwrite=self.overwrite, append=self.append)
        self.file = open(self.filename, mode=self.mode)
        
        self.keep_left = True
        self.offset_cursor = False
        if self.mode == 'rb+':
            self.file.seek(0, os.SEEK_END) # go to end of file
            if self.file.tell() != 0: # if current position is truish (i.e != 0) so file is not empty       
                self.keep_left = False
                self.offset_cursor = True
            else:
                self.file.seek(0)
        
        
    def write(self, data: List[dict]):

        output = json.dumps(data, indent=INDENT)
        
        END = -len(SEP)-len(']')
        if self.keep_left:
            self.keep_left = False
            output = output[:END] # remove last closed bracket `\n]`
        else:
            START = len(SEP)+INDENT+len(']')
            output = f",{SEP}{' '*INDENT}{output[START:END]}"
        
        # position the cursor for proper appending
        if self.offset_cursor:
            self.offset_cursor = False
            self.file.seek(END, os.SEEK_END)
        
        self.file.write(output.encode())
    
    def __del__(self):
        self.file.write(f'{SEP}]'.encode())
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
    
    
    saved_data = json.load(open(filename, mode='r'))
    print(saved_data == data)
    print(saved_data)
    print(data)
    
    
def test_rb_ab():
    file = open('tt.json', mode='wb+')
    file.write('ABCDEFGHIJKLMONQRSTUVWXYZ'.encode())
    file.close()
    
    file = open('tt.json', mode='rb+')
    file.seek(-10, os.SEEK_END)
    file.write('123456789'.encode())
    file.close()
    
def test_storelist_json():
    l = StoreListJson('store.json', frequency=5, overwrite=True)
    
    for i in range(99):
        l.push({'index': i, 'data': i**2})
       
if __name__ == '__main__':
    
    test_json_driver()

