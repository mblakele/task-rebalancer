xquery version "1.0-ml";
(:
 : Copyright (c) 2011-2013 Michael Blakeley. All Rights Reserved.
 :
 : Licensed under the Apache License, Version 2.0 (the "License");
 : you may not use this file except in compliance with the License.
 : You may obtain a copy of the License at
 :
 : http://www.apache.org/licenses/LICENSE-2.0
 :
 : Unless required by applicable law or agreed to in writing, software
 : distributed under the License is distributed on an "AS IS" BASIS,
 : WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 : See the License for the specific language governing permissions and
 : limitations under the License.
 :
 : The use of the Apache License does not indicate that this project is
 : affiliated with the Apache Software Foundation.
 :
 :)

import module namespace trb="com.blakeley.task-rebalancer"
  at "/lib-trb.xqy" ;

(: This version of forests-uris.xqy is designed to evacuate a forest.
 : After all uri-tasks have been run, the target forest should be empty.
 :
 : NOTE
 : This script is for test purposes only.
 : In real life, set the forest updates-allowed=delete-only
 : and invoke forests.xqy instead.

xdmp:spawn(
  "test/forest-uris-evacuate.xqy",
  (xs:QName('FOREST'), xdmp:forest('forest-to-evacuate'),
   xs:QName('INDEX'), -1,
   xs:QName('LIMIT'), 0,
   xs:QName('RESPAWN'), true()),
  <options xmlns="xdmp:eval">
    <database>{ xdmp:database() }</database>
    <root>/path/to/mblakele-task-rebalancer/</root>
    <time-limit>3600</time-limit>
  </options>)

 :)

(: The forest to empty out. :)
declare variable $FOREST as xs:unsignedLong external ;

(: The forest(s) to target.
 : Current forest is not eligible for placement.
 :)
declare variable $TARGETS as xs:unsignedLong+ := trb:database-forests()[
  not(. eq $FOREST)] ;

declare variable $LIMIT as xs:integer external ;

declare variable $RESPAWN as xs:boolean external ;

(: Clear any state. :)
trb:uris-start-set($FOREST, ()),

trb:spawn(
  'forest-uris-evacuate.xqy',
  $FOREST,
  (: never match existing documents :)
  -1,
  xdmp:forest-status($FOREST),
  $TARGETS,
  $RESPAWN,
  $LIMIT)

(: forest-uris-evacuate.xqy :)
