Create custom models for OMERO.biobank
======================================

OMERO XML models
----------------

Creating new data models for an OMERO server is quite easy, data
structures are defined in XML files with a .ome.xml extension stored in
$OMERO\_SERVER\_HOME/components/model/resoruces/mappings/ directory.

OMERO models use XML 1.0 syntax, so each file must begin with the header

.. code-block:: xml

  <?xml version="1.0" encoding="UTF-8"?>

data structures must be contained between tags

.. code-block:: xml

  <types>
    .....
    .....
  </types>

Each model (or data structure) can be a “type” or an “enum”, “type”s can
be seen as the XML representation of SQL tables, “enum”s are special
tables with only two columns: an ID and a String value.

To define an enum just create a model like

.. code-block:: xml

  <enum id="ome.model.vl.VesselContent">
    <entry name="EMPTY"/>
    <entry name="BLOOD"/>
    <entry name="SERUM"/>
    <entry name="DNA"/>
    <entry name="RNA"/>
  </enum>

the ID field of each row will be automatically assigned by the server
when the model will be deployed.

To define a type create a model like

.. code-block:: xml

  <type id="ome.model.vl.Vessel">
    <properties>
      <required name="vid" type="string" unique="true"/>
      <required name="activationDate" type="timestamp"/>
      <optional name="destructionDate" type="timestamp"/>
      <required name="currentVolume" type="float"/>
      <required name="initialVolume" type="float"/>
      <required name="content" type="ome.model.vl.VesselContent"/>
      <required name="status" type="ome.model.vl.VesselStatus"/>
      <required name="action" type="ome.model.vl.Action"/>
      <optional name="lastUpdate" type="ome.model.vl.Action"/>
    </properties>
  </type>

models can be also extended like classes in Object Oriented programming
languages

.. code-block:: xml

  <type id="ome.model.vl.Tube" superclass="ome.model.vl.Vessel">
    <properties>
      <required name="label" type="string" unique="true"/>
      <optional name="barcode" type="string" unique="true"/>
    </properties>
  </type>

a child model inherits all the parameters of its superclasses but
cannot override parameters of the superclasses.

Every model has to start with a

.. code-block:: xml

  <type id="...">

tag and all the properties must be enclosed between

.. code-block:: xml

  <properties>
    ....
    ....
  </properties>


Models constraints
~~~~~~~~~~~~~~~~~~

Each property must be required or optional, only optional fields can
have a null value.

A unique key can be specified by adding the unique=“true” constraint to
the property, unfortunately OMERO does not allow to specify a
multi-field unique key (like SQL language does), in order to use such a
key a new field must be introduced in the model like

.. code-block:: xml

  <type id="ome.model.vl.PlateWell" superclass="ome.model.vl.Vessel">
    <properties>
      <required name="label" type="string"/>
      <required name="slot" type="int"/>
      <required name="container" type="ome.model.vl.TiterPlate"/>
      <!-- container.label, label -->
      <required name="containerSlotLabelUK" type="string" unique="true"/>
      <!-- container.label, slot -->
      <required name="containerSlotIndexUK" type="string" unique="true"/>
    </properties>
  </type>

the **containerSlotLabelUK** and **containerSlotIndexUK** are two unique
keys for the PlateWell object, each one is used to combine two different
fields. In order to make easier to understand which field the key will
combine, we add a comment with the used fields and the order used to
combine fields values in order to obtain the key.

Properties’ types
~~~~~~~~~~~~~~~~~

Each property of a model can be one of the following types:

* string
* text
* int
* long
* float
* timestamp
* boolean

or a model itself. To specify a model as property type just use the
model ID in the type property. Foreign keys are automatically managed by
OMERO’s hibernate engine.

OMERO.biobank modelling conventions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Each model, except for the enumerations, must have a VID attribute.
  The VID is a secondary ID for the object automatically assigned by
  the biobank API that is used as a substitute of the automatically
  assigned OMERO ID.

* Each object that must appear in the “chain of custody system” must
  have an “action” field and a “lastUpdate” field. The action field is
  use to track the object’s source (if the object has not a source the
  action field is used to register that the object as been imported
  without a parent object), the lastUpdate object is used to keep
  trace of all updates occurred to the object itself in order to keep
  trace of the object’s history.

OMERO.biobank models wrapping
-----------------------------

In order to use the new data structures, new models must be wrapped in
OMERO.biobank. Wrapping a new model is quite simple, a new model only
has to extend the bl.vl.kb.drivers.omero.wrapper.OmeroWrapper class. All
modules defining object mappings must be contained in the
bl.vl.kb.driver.omero package.

To wrap the above VesselContent enumeration

.. code-block:: python

  import wrapper as wp

   class VesselContent(wp.OmeroWrapper):

    OME_TABLE = 'VesselContent'
    __enums__ = ['EMPTY', 'BLOOD', 'SERUM', 'DNA', 'RNA']

where

* OME\_TABLE is the id of the model without the namespace specified
  in the XML file

* *enums* is the list of the strings contained in the enumeration that
  has been wrapped; it is important that the strings of the list match
  the cases of the strings specified in the XML model.

To wrap the Vessel model above

.. code-block:: python

    import wrapper as wp
    from Action import Action
    from utils import assign_vid_and_timestamp, assign_vid

    class Vessel(wp.OmeroWrapper):

      OME_TABLE = 'Vessel'
      __fields__ = [('vid',   wp.VID, wp.REQUIRED),
                    ('activationDate', wp.TIMESTAMP, wp.REQUIRED),
                    ('destructionDate', wp.TIMESTAMP, wp.OPTIONAL),
                    ('currentVolume', wp.FLOAT, wp.REQUIRED),
                    ('initialVolume', wp.FLOAT, wp.REQUIRED),
                    ('content', VesselContent, wp.REQUIRED),
                    ('status', VesselStatus, wp.REQUIRED),
                    ('action', Action, wp.REQUIRED),
                    ('lastUpdate', Action, wp.OPTIONAL)]

      def __preprocess_conf__(self, conf):
        if not 'activationDate'  in conf:
          return assign_vid_and_timestamp(conf, time_stamp_field='activationDate')
        else:
          return assign_vid(conf)

and to map the PlateWell model that extends the Vessel model

.. code-block:: python

    import wrapper
    from utils import make_unique_key

    class PlateWell(Vessel):

      OME_TABLE = 'PlateWell'
      __fields__ = [('label', wp.STRING, wp.REQUIRED),
                    ('slot', wp.INT, wp.REQUIRED),
                    ('container', TiterPlate, wp.REQUIRED),
                    ('containerSlotLabelUK', wp.STRING, wp.REQUIRED),
                    ('containerSlotIndexUK', wp.STRING, wp.REQUIRED)]

      def __preprocess_conf__(self, conf):
        super(PlateWell, self).__preprocess_conf__(conf)
        if not 'containerSlotLabelUK' in conf:
          clabel = conf['container'].label
          label   = conf['label']
          conf['containerSlotLabelUK'] = make_unique_key(clabel, label)
        if not 'containerSlotIndexUK' in conf:
          clabel = conf['container'].label
          slot   = conf['slot']
          conf['containerSlotIndexUK'] = make_unique_key(clabel, '%04d' % slot)
        return conf

      def __update_constraints__(self):
        csl_uk = make_unique_key(self.container.label, self.label)
        setattr(self.ome_obj, 'containerSlotLabelUK',
                self.to_omero(self.__fields__['containerSlotLabelUK'][0], csl_uk))
        csi_uk = make_unique_key(self.container.label, '%04d' % self.slot)
        setattr(self.ome_obj, 'containerSlotIndexUK',
                self.to_omero(self.__fields__['containerSlotIndexUK'][0], csi_uk))

in this case we have a list called *fields* which map all the fields
specified in the XML model.

The syntax for element of the list is is

::

    ( field_label, field_type, field_constraint )

where

* field\_label is the string specified in the “name” attribute of the
  field in the XML file

* field\_type can be:

  * wp.VID for the VID field
  * wp.TIMESTAMP for timestamp fields
  * wp.STRING for string fields
  * wp.TEXT for text fields
  * wp.FLOAT for float fields
  * wp.INT for int fields
  * wp.LONG for long fields
  * wp.BOOLEAN for boolean fields
  * a class that extends the OmeroWrapper class if you want to

associate another model to the field

* field\_constraint can be:

  * wp.REQUIRED for required fields
  * wp.OPTIONAL for optional fields

OmeroWrapper also defines some functions that are automatically called
when managing objects that wrap OMERO models:

* def ``__preprocess_conf__``\ (self, conf): called when an object is
  created; it is used to automatically assign fields like a timestamp
  or a VID if the field is not specified in the object’s configuration
  or it can be used to validate object’s configuration.

* def ``__update_constraints__``\ (self): called every time an
  object’s field has been updated; it is used to automatically
  recalculate unique keys constraints.

* def ``__cleanup__``\ (self): it is called after an objects has been
  deleted; it is used to cleanup OMERO’s database or the graph after
  an object has been successfully deleted. PAY ATTENTION when
  overriding this function because this is the only one with an
  implementation in the OmeroWrapper class and it is used to remove
  nodes and edges from the graph engine when an object or an action
  has been deleted from OMERO.


Unique keys creation
~~~~~~~~~~~~~~~~~~~~

In order to make simple and standard the creation of a unique key, a
make\_unique\_key function has been defined in the
bl.vl.utils.ome\_utils module; the function takes a list of strings as
argument, joins them and returns the hash digest of the resulting string
that will be used as unique key. This function is usually used in the
``__preprocess_conf__`` and in the ``__update_constraints__`` functions
of the wrapping object. Make sure that, every time the make\_unique\_key
function is called, the strings of the input list are passed in the same
order.
