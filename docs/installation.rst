Installation instructions
=========================


Omero Setup
-----------

Add test group and user.

.. code-block:: bash

 > omero group list
  Server: [localhost]
  Username: [root]
  Password:
  Created session e... (xx:4064). Idle timeout: 10.0 min. Current group: system
  id | name   | perms  | # of owners | # of members 
  ----+--------+--------+-------------+--------------
  0  | system | rw---- | 1           | 0            
  1  | user   | rwr-r- | 0           | 1            
  2  | guest  | rw---- | 0           | 1            
  (3 rows)

  > omero group add --type read-only test
 Using session e.. (xx:4064). Idle timeout: 10.0 min. Current group: system
 Added group 3 (id=test) with permissions rwr---

  > omero group  list
  Using session e.. (xx:4064). Idle timeout: 10.0 min. Current group: system
  id | name   | perms  | # of owners | # of members 
  ----+--------+--------+-------------+--------------
  0  | system | rw---- | 1           | 0            
  1  | user   | rwr-r- | 0           | 1            
  2  | guest  | rw---- | 0           | 1            
  3  | test   | rwr--- | 0           | 0            
  (4 rows)

  > omero user list  
  Using session e.. (xx:4064). Idle timeout: 10.0 min. Current group: system
  id | omeName | firstName | lastName | email | member of | leader of 
  ----+---------+-----------+----------+-------+-----------+-----------
  0  | root    | root      | root     |       | 1         | 0         
  1  | guest   | Guest     | Account  |       | 2         |           
  (2 rows)


  > omero user add -m E -P test test Alfred Neumann test
  Using session e.. (xx:4064). Idle timeout: 10.0 min. Current group: system
  Added user 2 with password
  
  > omero user list    
  Using session e.. (xx:4064). Idle timeout: 10.0 min. Current group: system
  id | omeName | firstName | lastName | email | member of | leader of 
  ----+---------+-----------+----------+-------+-----------+-----------
  0  | root    | root      | root     |       | 1         | 0         
  1  | guest   | Guest     | Account  |       | 2         |           
  2  | test    | Alfred    | Neumann  |       | 3,1       |           
  (3 rows)


Installation checks
-------------------

Run::

 bash tests/tools/importer/test_importer.sh

