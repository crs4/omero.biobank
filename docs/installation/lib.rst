OMERO.biobank: client installation guide
========================================

This page will show how to install OMERO.biobank client API


Software dependencies
---------------------

Common dependencies
~~~~~~~~~~~~~~~~~~~

-  Python 2.7
-  OMERO Python libraries (with compiled custom models). Check out the
   OMERO server installation guide for a complete walkthrough on how to
   build OMERO server with custom models. After installation, add the
   Python libraries to your PYTHONPATH:
   export PYTHONPATH="${OMERO\_HOME}/dist/lib/python:${PYTHONPATH}"
-  `Ice <http://www.zeroc.com>`__ 3.3 or 3.4 Python libraries.
-  **NOTE:** all clients must use the same Ice version as the server
-  `NumPy <http://www.numpy.org>`__
-  `biodoop-core <https://github.com/crs4/biodoop-core>`__
-  **NOTE:** although `Pydoop <http://pydoop.sourceforge.net>`__ is
    listed as one of Biodoop Core's prerequisites, it's only a run-time
    dependency for MapReduce applications. Thus, Pydoop is currently
    **NOT** a requirement for OMERO.biobank.

With Neo4j and RabbitMQ enabled
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  `pika <http://pika.readthedocs.org>`__ 0.9.13

::
 
   pip install pika==0.9.13
 

-  `bulbs <http://bulbflow.com>`__ >= 0.3

:: 
   
   pip install bulbs
 

-  `voluptuous <https://pypi.python.org/pypi/voluptuous>`__ >= 0.7.1

:: 
   
    pip install voluptuous
 

With Neo4j and RabbitMQ disabled
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  `pygraph <https://code.google.com/p/python-graph>`__ >= 1.8.0

::

   easy\_install python-graph-core

Configuration
-------------

Configuration parameters related to Neo4j and RabbitMQ must be retrieved
from the OMERO server:

.. raw:: html

   <pre>
   $python {BIOBANK_REPO}/build_configuration.py -H ${OMERO_HOST} -U ${OMERO_USER} -P ${OMERO_PASSWD} --python
   </pre>

where:

-  ``${OMERO_USER}`` must be a user with admin privileges in the OMERO
   server
-  the ``--python`` flag sets the ``${BIOBANK_REPO}/bl/vl/kb/config.py``
   as output for the script: in this way, Neo4j and RabbitMQ config
   values will be installed while installing the libraries.

If you want to interact with more than one OMERO server, use
``--profile`` instead of ``--python``: the script will produce an
``${OMERO_HOST}.profile`` file that can be sourced to load config values
as environment variables.

Installation
------------

System-wide:

.. raw:: html

   <pre>
   sudo ${BIOBANK_REPO}/setup.py install
   </pre>

Local (i.e., into ``~/.local/lib/python2.7/site-packages``):

.. raw:: html

   <pre>
   ${BIOBANK_REPO}/setup.py install --user
   </pre>


