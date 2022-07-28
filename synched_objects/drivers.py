import json
import os
from pathlib import Path
from typing import List
from abc import ABC, abstractmethod

from .utils import assert_type

INDENT = 4
SEP = '\n'


class Driver(ABC):
    
    @abstractmethod
    def write(self, data: List[dict]):
        pass

class JsonDriver(Driver):
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
            # Check if file is not empty
            
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
        
        assert isinstance(data, list)
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
        self.file.flush()
        self.isempty = False
    
    def __del__(self):
        # self.file.flush()
        self.file.close()
