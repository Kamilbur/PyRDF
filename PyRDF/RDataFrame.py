from __future__ import print_function
from .Node import Node
import ROOT

class RDataFrame(Node):
    """
    The Python equivalent of ROOT C++'s
    RDataFrame class.

    Attributes
    ----------
    args
        A list of arguments that were provided to
        construct the RDataFrame object.

    Supported constructor arguments
    -------------------------------
    PyRDF's RDataFrame constructor accepts the same arguments as the ROOT's RDataFrame
    constructor (see https://root.cern/doc/master/classROOT_1_1RDataFrame.html). In addition,
    PyRDF allows you to use Python lists in place of C++ vectors as arguments of the constructor,
    e.g., `RDataFrame("myTree", ["file1.root", "file2.root"]`.

    Raises
    ------
    RDataFrameException
        An exception raised when input arguments to
        the RDataFrame constructor are incorrect.

    """

    def __init__(self, *args):
        """
        Creates a new RDataFrame instance for
        the given arguments.

        Parameters
        ----------
        *args
            Variable length argument list to construct
            the RDataFrame object.

        """

        super(RDataFrame, self).__init__(None, None)

        args = list(args) # Make args mutable
        num_params = len(args)

        for i in range(num_params):
            # Convert Python list to ROOT CPP vector
            if isinstance(args[i], list):
                args[i] = self._get_vector_from_list(args[i])

        try:
            ROOT.ROOT.RDataFrame(*args) # Check if the args are correct
        except TypeError as e:
            rdf_exception = RDataFrameException(e, "Error creating the RDataFrame !")
            rdf_exception.__cause__ = None
            # The above line is to supress the traceback of error 'e'
            raise rdf_exception

        self.args = args


    def _get_vector_from_list(self, arg):
        """
        Converts a python list of strings
        to a vector.

        """
        reqd_vec = ROOT.std.vector('string')()

        for elem in arg:
            reqd_vec.push_back(elem)

        return reqd_vec

    def get_num_entries(self):
        """
        Gets the number of entries in
        the given dataset.

        Returns
        -------
        int
            This is the computed number of entries
            in the input dataset.

        """
        first_arg = self.args[0]
        if isinstance(first_arg, int):
            # If there's only one argument
            # which is an integer, return it.
            return first_arg
        elif isinstance(first_arg, ROOT.TTree):
            # If the argument is a TTree or TChain,
            # get the number of entries from it.
            return first_arg.GetEntries()

        second_arg = self.args[1]

        # Construct a ROOT.TChain object
        chain = ROOT.TChain(first_arg)

        if isinstance(second_arg, str):
            # If the second argument is a string
            chain.Add(second_arg)
        else:
            # If the second argument is a list or vector
            for fname in second_arg:
                chain.Add(fname)

        return chain.GetEntries()

    def get_treename(self):
        """
        Get name of the TTree

        Returns
        -------
        str or None
            Name of the TTree, or None if there is no tree.

        """
        first_arg =  self.args[0]
        if isinstance(first_arg, ROOT.TChain):
            # Get name from a given TChain
            return first_arg.GetName()
        elif isinstance(first_arg, ROOT.TTree):
            # Get name directly from the TTree
            return first_arg.GetUserInfo().At(0).GetName()
        elif isinstance(first_arg, str):
            # First argument was the name of the tree
            return first_arg
        # RDataFrame may have been created without any TTree or TChain
        return None

    def get_inputfiles(self):
        """
        Get list of input files

        This list can be extracted from a given TChain or from the list of
        arguments.

        Returns
        -------
        str, list or None
            Name of a single file, list of files (both may contain globbing
            characters), or None if there are no input files.

        """
        first_arg = self.args[0]
        if isinstance(first_arg, ROOT.TChain):
            # Extract file names from a given TChain
            chain = first_arg
            return [chainElem.GetTitle() for chainElem in chain.GetListOfFiles()]
        if len(self.args) > 1:
            second_arg = self.args[1]
            if isinstance(second_arg, str) or isinstance(second_arg, ROOT.std.vector('string')):
                # Get file(s) from second argument (may contain globbing characters)
                return second_arg
        # RDataFrame may have been created with no input files
        return None

class RDataFrameException(Exception):
    """
    A special type of Exception
    that shows up for incorrect
    arguments to RDataFrame.

    """
    def __init__(self, exception, msg):
        """
        Creates a new `RDataFrameException`.

        Parameters
        ----------
        exception
            An exception of type `Exception` or
            any child class of `Exception`.

        msg : str
            Message to be printed while raising exception

        """
        super(RDataFrameException, self).__init__(exception)
        print(msg)
