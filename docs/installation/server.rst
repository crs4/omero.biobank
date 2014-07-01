OMERO.biobank: server installation guide
========================================

This page will show how to install the OMERO.biobank server


Software dependencies
---------------------

-  `Oracle
   JDK <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__
   >= 1.5
-  `Ice <http://www.zeroc.com/>`__ 3.3 or 3.4 (version 3.4 is highly
   recommended)
-  make sure you install both Java and Python libraries when installing
   Ice
-  `Numpy <http://www.numpy.org>`__
-  `PyTables <https://pypi.python.org/pypi/tables>`__ to enable
   `OMERO.tables <http://www.openmicroscopy.org/site/support/omero4/developers/Tables.html>`__

Getting the source code
-----------------------

::

    git clone --recursive git://github.com/openmicroscopy/openmicroscopy.git

switch to the master branch:

::

    cd ${OMERO_HOME}
    git checkout master

The master branch will usually correspond to the latest stable OMERO
release. If you need a specific release, you can switch to it by using
the following command:

::

    git reset --hard ${VERSION}

To check all available versions:

::

    git tag -l

Finally, update the submodules:

::

    git submodule update

Setting up the database
-----------------------

Refer to the `official
documentation <http://www.openmicroscopy.org/site/support/omero4/sysadmins/unix/server-installation.html#creating-a-database-as-root>`__

Deploying the custom Biobank models
-----------------------------------

Check out the OMERO.biobank source code:

::

    git clone https://github.com/crs4/omero.biobank.git

and copy the custom model files to OMERO's model repository

::

    cp ${BIOBANK_REPO}/models/*.ome.xml ${OMERO_HOME}/components/model/resources/mappings/

Configuring the server
----------------------

Database connection
~~~~~~~~~~~~~~~~~~~

Edit the ``${OMERO_HOME}/etc/omero.properties`` file:

::

    omero.db.host=${POSTGRESQL_HOST}
    omero.db.name=${OMERO_DB_NAME}
    omero.db.user=${OMERO_DB_USER}
    omero.db.pass=${OMERO_DB_PASSWD}

OMERO data directory
~~~~~~~~~~~~~~~~~~~~

You can set the OMERO data directory in
``${OMERO_HOME}/etc/omero.properties``, **but this works only as a
reminder**. We will soon show how to configure it through the admin
shell.

Patching the OMERO version
~~~~~~~~~~~~~~~~~~~~~~~~~~

Since OMERO.biobank runs on a modified version of OMERO that uses custom
models, a specific tag must be added to the ``omero.version`` property.
This allows to perform a version check to ensure that the OMERO server
supports the specific set of models used by the client. To apply the
patch, run:

::

    ./utils/patch_ome_config --ome-home ${OMERO_HOME} --models-repository ${MODELS_REPOSITORY}

where ``${MODELS_REPOSITORY}`` can be the ``models`` directory of the
repository.

Increasing the amount of memory allocated to the Blitz process
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to handle large datasets, you may need to increase the memory
assigned to the Blitz process. Open
``${OMERO_HOME}/etc/grid/templates.xml`` with a text editor, look for
the ``<server-template id="BlitzTemplate">`` tag and edit the following
lines:

::

    <server-template id="BlitzTemplate">
    ...
        <target name="Blitz-hprof">
        ...
        </target>
        <option>-Xmx${MAX_BLITZ_MEM_IN_MB}M</option>
        <option>-XX:MaxPermSize=${MAX_PERM_SIZE_IN_MB}m</option>
    ...

The ``MaxPermSize`` parameter is required by Hibernate and represents an
extra amount of memory that will be assigned to the Blitz server.

Increasing the maximum size of Ice messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To handle large results from a single query, you need to increase Ice's
maximum message size.

In ``${OMERO_HOME}/components/blitz/resources/omero/Constants.ice``,
set:

::

    const int MESSAGESIZEMAX = ${MAX_MESSAGE_SIZE_IN_MB};

In ``${OMERO_HOME}/etc/ice.config``:

::

    Ice.MessageSizeMax=${MAX_MESSAGE_SIZE_IN_MB}

In ``${OMERO_HOME}/etc/grid/templates.xml``:

::

    <properties id="Basics">
      <property name="Ice.MessageSizeMax" value="${MAX_MESSAGE_SIZE_IN_MB}"/>
      ...
    </properties>

Graph DB and Message engines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

OMERO.biobank can use a graph DB to take advantage of a more reliable
and faster lookup engine for the retrieval of the "chain of custody" of
the various objects stored in the system. The libraries also need a
message engine to synchronize data between the OMERO database and the
graph DB.

We use `Neo4j <http://www.neo4j.org>`__ as graph DB and
`RabbitMQ <http://www.rabbitmq.com>`__ as message engine. To install
these tools, refer to the official documentation:

-  `Neo4j install guide <http://www.neo4j.org/download>`__
-  `RabbitMQ install guide <http://www.rabbitmq.com/download.html>`__

Neo4j and RabbitMQ configuration parameters must be set at the end of
``$OMERO_HOME/etc/omero.properties`` (these values will be retrieved
when installing the OMERO.biobank API):

::

    ...
    omero.biobank.graph.engine=neo4j
    omero.biobank.graph.uri=${NEO4J_SERVER_URI}
    omero.biobank.graph.user=${NEO4J_USER} #optional
    omero.biobank.graph.password=${NEO4J_PASSWORD} #optional
    omero.biobank.messages_queue.enabled=True
    omero.biobank.messages_queue.host=${RABBITMQ_SERVER_HOST}
    omero.biobank.messages_queue.port=${RABBITMQ_SERVER_PORT} #optional
    omero.biobank.messages_queue.queue=${RABBITMQ_QUEUE_NAME}
    omero.biobank.messages_queue.user=${RABBITMQ_USER} #optional
    omero.biobank.messages_queue.password=${RABBITMQ_PASSWORD}  #optional

**NOTE 1**: all variables listed as optional must be set only if you
change the default Neo4j/RabbitMQ configuration.

**NOTE 2**: to set the above parameters when the server is already
installed, use the ``omero config`` command (see below)

If you don't want to enable Neo4j and RabbitMQ (e.g., on a test server),
append the following settings to ``${OMERO_HOME}/etc/omero.properties``:

::

    ...
    omero.biobank.graph.engine=pygraph
    omero.biobank.messages_queue.enabled=False

Building the server and the DB schema
-------------------------------------

Build the server:

::

    ${OMERO_HOME}/build.py

The build process may fail with the following error:

::

    The system is out of resources.
    Consult the following stack trace for details.
    java.lang.OutOfMemoryError: GC overhead limit exceeded
    [...]

In this case, open ``${OME_HOME}/build.py`` with a text editor and
increase the the value for ``-Djavac.maxmem.default`` in
``calculate_memory_args``. For instance:

.. code-block:: python

    def calculate_memory_args():
        return "-Xmx600M -Djavac.maxmem.default=1024M -Djavadoc.maxmem.default=750M -XX:MaxPermSize=256m".split(" ")

Build the DB schema from hibernate classes:

::

    ${OMERO_HOME}/build.py build-schema -Domero.db.dialect=org.hibernate.dialect.PostgreSQLDialect

Generate the SQL code to initialize the DB:

::

    ${OMERO_HOME}/dist/bin/omero db script

The above command will prompt for the omero DB version and patch (press
enter to use the suggested default values) and the root password.

Deploy the SQL file to the PostgreSQL server:

::

    psql -h ${POSTGRESQL_HOST} -U ${DB_USER} ${DB_NAME} < ${OMERO_SQL_FILE}

configure the data directory

::

    ${OMERO_HOME}/dist/bin/omero config set omero.data.dir ${OMERO_DATA_DIR}

Starting the server
-------------------

Start the OMERO server with the following command:

::

    ${OMERO_HOME}/dist/bin/omero admin start

Starting the graph manager
~~~~~~~~~~~~~~~~~~~~~~~~~~

If you enabled Neo4j, you will also have to start the graph manager, a
daemon that updates the Neo4j database according to the RabbitMQ queue
status:

::

    ${BIOBANK_HOME}/daemons/graph_manager.py

**NOTE:** to run the daemon, you have to install OMERO.biobank first.
