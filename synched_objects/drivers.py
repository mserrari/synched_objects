import json
import os
from pathlib import Path
from typing import Dict, List
from abc import ABC, abstractmethod
import sqlite3
from sqlite3 import Error as SQLError

from .utils import assert_type

INDENT = 4
SEP = '\n'


class Driver(ABC):
    """ Abstract class for Drivers """
    
    @abstractmethod
    def write(self, data: List[Dict]):
        """ Method to write depending on the specific implementation """
        pass

class JsonDriver(Driver):
    """ Json driver that supports appending to existing jsons """
    
    def __init__(self, filename: str, overwrite: bool = False, append: bool = False) -> None:
        super().__init__()
        
        assert_type(filename, 'filename', (str, Path))
        assert_type(overwrite, 'overwrite', bool)
        assert_type(append, 'append', bool)
        
        self.filename = Path(filename)
        self.overwrite = overwrite
        self.append = append
        
        self.__post_init__()
            
    def __post_init__(self):
        """ Create or open file then trunctate or append """
        
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

    def write(self, data: List[Dict]):
        
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
        self.file.flush()
        self.file.close()


class SQLDriver(Driver):
    
    __table__ = 'data'
    
    def __init__(self, dbname: str) -> None:
        super().__init__()
        
        self.cols = None
        
        try:
            
            Path(dbname).unlink(missing_ok=True)
            self.conn = sqlite3.connect(dbname)
            self.cursor = self.conn.cursor()
        except SQLError as e:
            raise(e)       
        
    def parser(self, item: Dict) -> List:
        
        if self.cols is None:
            self.init_db(item)
        else:
            assert self.cols == set(item), f'item must have the same keys as previously \
                inserted items. Expected {self.cols} got {set(item)}'
                
        return [type_func(item[key]) for key, type_func in self.types.items()]
    
    def init_db(self, item):
        
        self.cols = set(item)
        self.type_mapping = {
            int: 'INTEGER',
            str: 'TEXT',
            float: 'FLOAT',
            bool: 'BOOL'
        }
        
        object_names = []
        self.types = {}
        self.ordered_keys = []
        for k, v in item.items():
            
            self.ordered_keys.append(k)
            
            type_func = str if v is None else type(v)
            if type_func not in self.type_mapping:
                raise TypeError(f'Unsupported object type {type_func}, expected {self.type_mapping.keys()}')
            
            self.types[k] = type_func
            object_names.append(f'{k} {self.type_mapping[type_func]}')
        
        self.sql_create_table = f"""CREATE TABLE {self.__table__} 
            (id integer PRIMARY KEY, {', '.join(object_names)} );"""
            
        self.sql_insert_item = f"""INSERT INTO {self.__table__} 
            {tuple(self.types)} VALUES ({','.join(['?']*len(self.types))})"""
        
        try:
            self.cursor.execute(self.sql_create_table)
        except SQLError as e:
            raise(e)


    
    def write(self, data: List[Dict]):
        
        parsed_data = [self.parser(item) for item in data]
        self.cursor.executemany(self.sql_insert_item, parsed_data)
        
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def get_data(self) -> List[Dict]:
        self.cursor.execute(f'SELECT * FROM {self.__table__}')
        rows = self.cursor.fetchall()
        return [{k: v for k, v in zip(self.ordered_keys, row[1:])} for row in rows]
    
    def __str__(self) -> str:
        return str(self.get_data())
        