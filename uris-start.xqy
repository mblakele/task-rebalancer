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
  at "lib-trb.xqy" ;

declare variable $FOREST as xs:anyAtomicType external ;

xdmp:log(
  text {
    'uris-start.xqy:', $FOREST,
    xdmp:describe(
      trb:uris-start(
        if ($FOREST castable as xs:unsignedLong) then xs:unsignedLong($FOREST)
        else xdmp:forest($FOREST))) }, 'info')

(: uris-start :)
