Developing Galaxy interfaces
============================

FIXME this is currently only a note to jot down ideas on how we should
interface with galaxy.

In this section, we will discuss how one could use galaxy to provide
simple but functional interfaces to omero/VL functionalities.

Templating options
------------------

.. code-block:: xml

 <command interpreter="python">
   data_source.py $output $__app__.config.output_size_limit
 </command>

.. code-block:: xml

    <outputs>
        <data name="output" format="txt" 
	  label="${tool.name} on $getVar( 'q', 'unknown position' )"/>
    </outputs>
    <options sanitize="False" refresh="True"/>

.. code-block:: xml

    <outputs>
        <data name="output" format="tabular" 
	  label="${tool.name} on ${organism}: ${table} (#if $description == 'range' then $getVar( 'position', 'unknown position' ) else $description#)"/>
    </outputs>

.. code-block:: xml

    <outputs>
        <data name="output" format="tabular" 
	label="${tool.name} on ${organism}: ${table} (#if $description == 'range' then $getVar( 'position', 'unknown position' ) else $description#)"/>
    </outputs>

.. code-block:: xml

    <outputs>
        <data format="bed" name="insertions" label="${tool.name} on ${on_string}: insertions" from_work_dir="tophat_out/insertions.bed">
            <filter>
                (
                    ( ( 'sParams' in singlePaired ) and ( 'indel_search' in singlePaired['sParams'] ) and 
                      ( singlePaired['sParams']['indel_search']['allow_indel_search'] == 'Yes' ) ) or 
                    ( ( 'pParams' in singlePaired ) and ( 'indel_search' in singlePaired['pParams'] ) and 
                      ( singlePaired['pParams']['indel_search']['allow_indel_search'] == 'Yes' ) )
                ) 
            </filter>
            <actions>
              <conditional name="refGenomeSource.genomeSource">
                <when value="indexed">
                  <action type="metadata" name="dbkey">
                    <option type="from_data_table" name="tophat_indexes" column="1" offset="0">
                      <filter type="param_value" column="0" value="#" compare="startswith" keep="False"/>
                      <filter type="param_value" ref="refGenomeSource.index" column="0"/>
                    </option>
                  </action>
                </when>
                <when value="history">
                  <action type="metadata" name="dbkey">
                    <option type="from_param" name="refGenomeSource.ownFile" param_attribute="dbkey" />
                  </action>
                </when>
              </conditional>
            </actions>
        </data>
        <data format="bed" name="deletions" label="${tool.name} on ${on_string}: deletions" from_work_dir="tophat_out/deletions.bed">
            <filter>
                (
                    ( ( 'sParams' in singlePaired ) and ( 'indel_search' in singlePaired['sParams'] ) and 
                      ( singlePaired['sParams']['indel_search']['allow_indel_search'] == 'Yes' ) ) or 
                    ( ( 'pParams' in singlePaired ) and ( 'indel_search' in singlePaired['pParams'] ) and 
                      ( singlePaired['pParams']['indel_search']['allow_indel_search'] == 'Yes' ) )
                )
            </filter>
            <actions>
              <conditional name="refGenomeSource.genomeSource">
                <when value="indexed">
                  <action type="metadata" name="dbkey">
                    <option type="from_data_table" name="tophat_indexes" column="1" offset="0">
                      <filter type="param_value" column="0" value="#" compare="startswith" keep="False"/>
                      <filter type="param_value" ref="refGenomeSource.index" column="0"/>
                    </option>
                  </action>
                </when>
                <when value="history">
                  <action type="metadata" name="dbkey">
                    <option type="from_param" name="refGenomeSource.ownFile" param_attribute="dbkey" />
                  </action>
                </when>
              </conditional>
            </actions>
        </data>
        <data format="bed" name="junctions" label="${tool.name} on ${on_string}: splice junctions">
            <actions>
              <conditional name="refGenomeSource.genomeSource">
                <when value="indexed">
                  <action type="metadata" name="dbkey">
                    <option type="from_data_table" name="tophat_indexes" column="1" offset="0">
                      <filter type="param_value" column="0" value="#" compare="startswith" keep="False"/>
                      <filter type="param_value" ref="refGenomeSource.index" column="0"/>
                    </option>
                  </action>
                </when>
                <when value="history">
                  <action type="metadata" name="dbkey">
                    <option type="from_param" name="refGenomeSource.ownFile" param_attribute="dbkey" />
                  </action>
                </when>
              </conditional>
            </actions>
        </data>
        <data format="bam" name="accepted_hits" label="${tool.name} on ${on_string}: accepted_hits">
            <actions>
              <conditional name="refGenomeSource.genomeSource">
                <when value="indexed">
                  <action type="metadata" name="dbkey">
                    <option type="from_data_table" name="tophat_indexes" column="1" offset="0">
                      <filter type="param_value" column="0" value="#" compare="startswith" keep="False"/>
                      <filter type="param_value" ref="refGenomeSource.index" column="0"/>
                    </option>
                  </action>
                </when>
                <when value="history">
                  <action type="metadata" name="dbkey">
                    <option type="from_param" name="refGenomeSource.ownFile" param_attribute="dbkey" />
                  </action>
                </when>
              </conditional>
            </actions>
        </data>
    </outputs>

