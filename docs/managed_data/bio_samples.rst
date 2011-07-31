How biosamples are modeled
==========================

A biosample is, within omero/vl, what is contained in a container that
can be identified and that can be linked, possibly after many up steps
along the dependency tree, to an individual. The individual can be a
man, a mouse, whatever, but there will be one.

The BioSample definition, is thus really a detailed description of the
state of a specific container and of its contents. 
It is essentially claiming that:

 * the sample is contained in a well identified container (currently
    only objects derived from Vessel);

 * the sample is of a specific bio fluid, choosen between the possible
   values of the enum VesselContent;

 * the sample definition links to an Action that described how the
   sample was produced/extracted/aliquoted and from where;

Moreover, the Vessel object keeps track, optionally, of its initial
and current volume.

FIXME add code examples?




