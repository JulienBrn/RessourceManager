"""
Summary
----------
Module defining abstract storages. There are two main classes that communicate using an `IO` object such as `io.BytesIO`, `io.StringIO`:    

    - :class:`AbstractStorage` which represents a storage space (directory on disk, space on server, database file, hdf5 file, share_memory, ...). 
        This class is responsible for creating the IO object (for example the :meth:`pathlib.Path.open`) at the right location.
    - :class:`AbstractIOProtocol` which handles how a python variable is exported/loaded to/from data (e.g. binary data, string, ...) to an IO Object.

They both have a listed set of `io_methods` with which they are compatible. 
The user of this module needs to find an io_method compatible with both ends. 
Then the tuple (:class:`AbstractStorage`, io_method, :class:`AbstractIOProtocol`) defines a storage solution to load and dump variables.

"""

from __future__ import annotations
from typing import Tuple, List, Union, Dict, Callable, Literal, TypeVar, Set, Any, NewType, Generic, NoReturn, Protocol
import io, pathlib, multiprocessing, inspect, functools



StorageLocation = TypeVar("StorageLocation")
AlphaNumString = NewType("AlphaNumString", str)
IO = NewType("IO", Any)


class AbstractStorage(Generic[StorageLocation], Protocol):
    """
    Abstract base class representing a storage.
    A storage may be a directory on disk, a directory on a server, a database, an hdf5 file, but also a dictionary in memory, a dictionary in memory in a different process, ...

    This class is meant to interact well with the AbstractIOProtocol class and the AbstractImportExportFormat class.

    Attributes
    ----------
        name: str
            Should be unique for every storage

    TODO
    ----------
        - define move function within and between storages. May require double_dispatch. Unclear whether this should be a free function, a static method or a method.
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
        ...

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
        ...
    
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
        ...
    
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
        ...
    
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
        ...
    
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
    
    @property
    def space(self) -> SpaceInformation:
        """
        Returns
        -------
        SpaceInformation
           the space information about the storage, mainly the methods :meth:`SpaceInformation.used_space` `SpaceInformation.remaining_space`.
        """
    
    @property
    def lifetime(self) -> LifetimeInformation:
        """
        Returns
        -------
        LifetimeInformation
           the lifetime information about the storage mainly the method `persists_after_execution` indicating whether the storage diseapears after execution.
        """
        ...

class LifetimeInformation(Protocol):
    def persists_after_execution(self) -> bool: ...

class SpaceInformation(Protocol):
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
        ...
    
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
    
class IOProtocol(Protocol):
    """
    Abstract base class representing a way to export/import python variables to storage.
    It handles the encoding and does the actual writing and reading of the data.

    Attributes
    ----------
        name: str
            **Should be unique for every protocol**. This is extremely important the same ressource exported using two different protocols (imagine a dataframe exported either with csv or parquet)
            should be treated as different different ressources on storage. This is usually done by passing the name of the protocol in the uniqueid used to generate the location of the ressource.

    """
     
    name: str

    def __init__(self, name):
        self.name = name

    def supports_dtype_method(t : type, io_method: str) -> bool:
        """
        Returns
        -------
        bool
           whether the given dtype is supported on the given io method. 

        Note 
        -------
        Many IOProtocols may simply not support some types (or even only handle a single type).
        In that case, it simply returns False for all io_method


        See Also
        -------
        :method:`load`, :method:`dump`, :class:`AbstractStorage` 
        """

    def dump(self, io: IO, obj: Any) -> NoReturn:
        """
        Dumps the object into ìo`, thus writing the object to storage (the location was determined when creating `io`)
        Parameters
        ----------
            io: IO
                The `io` object created by the storage with :meth:`AbstractStorage.mk_write_io`
            obj: Any
                The object to dump

        Notes
        ----------
        - The object must be of a supported type and the io must have been created with a supported io_method.
        Currently load does not take the io_method as a parameter. It is assumed that the needed information (possible the name of the method)
        is included in the IO object. It is yet unclear whether this is good design

        - It is assumed that any "closing" of handles (ssh connection, file handle, ...) inside IO is done here
        """
        ...
    def load(self, io: IO) -> Any:
        """
        Load the object from ìo`, thus reading the object from storage (the location was determined when creating `io`)
        Parameters
        ----------
            io: IO
                The `io` object created by the storage with :meth:`AbstractStorage.mk_read_io`

        Returns
        -------
        obj: Any
                The loaded object
        Notes
        ----------
        - The io must have been created with a supported io_method.
        Currently load does not take the io_method as a parameter. It is assumed that the needed information (possible the name of the method)
        is included in the IO object. It is yet unclear whether this is good design

        - It is assumed that any "closing" of handles (ssh connection, file handle, ...) inside IO is done here
        """
        ...
