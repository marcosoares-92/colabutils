__doc__ = """
Colab Utils: a simplified version of Industrial Data Science Workflow for using Colab.

Check the project Github: https://github.com/marcosoares-92/IndustrialDataScienceWorkflow
"""

__author__ = """Marco Cesar Prado Soares; Gabriel Fernandes Luz; Sergio Guilherme Neto"""
__version__ = "1.0.0"


from dataclasses import dataclass

class InvalidInputsError (Exception): pass  # class for raising errors when invalid inputs are provided.


@dataclass
class ControlVars:
    """
    Store the controls for plotting or showing results as Global variables to use.
    Plots and other not-returned elements are saved as ControlVars.

    for a variable var, syntax 
    if var:
        # is equivalent to: "run only when variable var is not None." or "run only if var is True".
    """
    # While show_plots = True, the system will print all plots in functions.
    # While show_results = True, the system prints all results.
    # User must change this variable state to create new connectors.
    show_plots = True
    show_results = True

"""Since these two classes are used by all of the modules, they must be initialized before module import.
If not, a circular import error will be raised, since the modules will try to import two classes that were not created yet.
"""

from .datafetch import *