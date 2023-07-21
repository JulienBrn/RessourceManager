"""
Summary
----------
Module defining abstract storages. There are two main classes that communicate using an `IO` object such as `io.BytesIO`, `io.StringIO`:    

    - :class:`AbstractStorage` which represents a storage space (directory on disk, space on server, database file, hdf5 file, share_memory, ...). 
        This class is responsible for creating the IO object (for example the :meth:`pathlib.Path.open`) at the right location.
    - :class:`AbstractIOProtocol` which handles how a python variable is exported/loaded to/from data (e.g. binary data, string, ...) to an IO Object.

They both have a listed set of `IO` objects with which they are compatible (called io_method). 
The user of this module needs to find an io_method compatible with both ends. 
Then the tuple (:class:`AbstractStorage`, IO object, :class:`AbstractIOProtocol`) defines a storage solution to load and dump variables.
The tuple is usually wrapped in the :class:`StorageSolution` class.

"""

from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn, Protocol
import io, pathlib, multiprocessing, inspect, functools



StorageLocation = TypeVar("StorageLocation")
AlphaNumString = NewType("AlphaNumString", str)
IO = NewType("IO", Any)
class LifetimeProtocol(Protocol):
    def persists_after_execution(self) -> bool: ...



class AbstractStorage(Generic[StorageLocation]):
    """
    Abstract base class for storage solutions.
    A storage may be a directory on disk, a directory on a server, a database, an hdf5 file, but also a dictionary in memory, a dictionary in memory in a different process, ...

    This class is meant to interact well with the AbstractIOProtocol class and the AbstractImportExportFormat class.

    Attributes
    ----------
        name: str
            Should be unique for every storage
    """
    name: str

    def __init__(self, name):
        self.name = name

    def get_ressource_location(self, uniqueid: AlphaNumString, name_info: Any = None) -> StorageLocation:
        """
        Returns where a ressource with uniqueid and name_info is stored on this storage. In other words, it transforms a unique_id into a location on this storage.
        This enables one to use the same uniqueid for different storages, and retrieve the corresponding location for each storage.

        Guarantees (even across executions)
        ----------
            - if unique_id1 != unique_id2 then s.get_ressource_location(unique_id1, name_info_x) != s.get_ressource_location(unique_id2, name_info_y)
            - s.get_ressource_location(unique_id, name_info) == s.get_ressource_location(unique_id, name_info) 
        
        Pitfalls 
        ----------
            The name_info parameter us purely decorative and is only optionaly used by the storage to provide more readable locations. 
            - To ensure that one gets a different location (necessary to store a different ressource),
            one should use a different unique_id. Simply using a different name_info is not enough.

            - To ensure that one gets the same location, one should pass the same unique_id and the same name_info
        
        Parameters
        ----------
            uniqueid: AlphaNumString
                the identifier of the location. Usually not human readable (result of an hash).
            name_info: Any, optional
                additional information that may be used by the storage to provide a more readable location.
                This is mainly used to specify the extension in case of a file storage.

         Returns
        -------
        StorageLocation
            The location of the ressource identified with uniqueid on this storage.
        """
        raise NotImplementedError()

    def has_ressource(self, location: StorageLocation) -> bool:
        """
        Checks whether a ressource is saved at location location.

        Parameters
        ----------
            location: StorageLocation
                the location of the ressource (normally created with :meth:`get_ressource_location`)

        Returns
        -------
        bool
           whether a ressource is saved at location location.
        """
        raise NotImplementedError()
    
    def mk_write_io(self, io_method: str, location: StorageLocation) -> IO:
        """
        Generates what can be used by a IOProtocol to write at the given location of the storage. 
        There may be several methods to write to a given storage and the selected method is specified by the io_method argument.
        The list of supported io_methods is specified by the :method:`supports_iomethod` method of the storage.

        Parameters
        ----------
            io_method: str
                the io method used to write to the storage. Examples of io_methods are:
                *binary_stream*, *string_stream*, *pathname* (avoid if possible), *function*, *ssh*, ...

            location: StorageLocation
                the location of the ressource (normally created with :meth:`~get_ressource_location`)

        Returns
        -------
        Any
           the IO object desired by the io_method
        """
        raise NotImplementedError()
    
    def mk_read_io(self, io_method: str, location: StorageLocation) -> IO:
        """
        Generates what can be used by a IOProtocol to read at the given location of the storage. 
        There may be several methods to write to a given storage and the selected method is specified by the io_method argument.
        The list of supported io_methods is specified by the :method:`supports_iomethod` method of the storage.

        Parameters
        ----------
            io_method: str
                the io method used to read to the storage. Examples of io_methods are:
                *binary_stream*, *string_stream*, *pathname* (avoid if possible), *function*, *ssh*, ...

            location: StorageLocation
                the location of the ressource (normally created with :meth:`~get_ressource_location`)

        Returns
        -------
        Any
           the IO object desired by the io_method
        """
        raise NotImplementedError()
    
    def supports_iomethod(method: str) -> bool:
        """
        Returns
        -------
        bool
           whether the given io method is supported by the storage. 
        
        See Also
        -------
        :method:`mk_read_io`, :method:`mk_write_io`, :class:`IOProtocol` 
        """
        raise NotImplementedError()
    
    def remove(self, locations: Set[StorageLocation], missing_ok=False) -> NoReturn:
        """
        Removes the ressources stored at the given locations

        Parameters
        ----------
            locations: Set
            missing_ok: bool
                behaves the same way as `pathlib.Path.unlink`
        """
        raise NotImplementedError()
    
    def remaining_space(self) -> float:
        """
        Returns
        -------
        float
           the estimated amount of remaning space in Gigabytes

        Note
        -------
            The remaning space of a storage may be shared with another storage, 
            for example when one has two storages defined on two different folders of the same disk.
        """
        raise NotImplementedError()
    
    def used_space(self, shared=False) -> float:
        """
         Parameters
        ----------
            shared: bool, default=False
                Only makes a difference if the storage is shared with other storages/systems/...

                If shared=False, returns the used space by the current storage

                If shared=True, returns the total space of the physical storage - the remaining space of the physical storage (= :meth:`remaining_space`).

        Returns
        -------
        float
           the estimated amount of used space in Gigabytes
        """
        raise NotImplementedError()
    
    def lifetime(self) -> LifetimeProtocol:
        """
        Returns
        -------
        LifetimeProtocol
           the lifetime information about the storage. This object has the method `persists_after_execution` indicating whether the storage diseapears after execution.
        """
        raise NotImplementedError()