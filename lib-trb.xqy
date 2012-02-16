xquery version "1.0-ml";

(:
 : Copyright (c) 2011-2012 Michael Blakeley. All Rights Reserved.
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

module namespace trb="com.blakeley.task-rebalancer" ;

declare default function namespace "http://www.w3.org/2005/xpath-functions" ;

(: We need a bail-out mechanism to stop the respawns.
 : This variable acts as a kill signal.
 :)
declare variable $FATAL := xdmp:get-server-field('TRB-FATAL') ;

declare variable $URIS-START := xdmp:get-server-field('TRB-URIS-START') ;

declare function trb:fatal-set($value as xs:boolean)
 as empty-sequence()
{
  xdmp:set-server-field('TRB-FATAL', $value),
  xdmp:set($trb:FATAL, $value)
};

declare function trb:maybe-fatal()
as empty-sequence()
{
  if (not($trb:FATAL)) then ()
  else error((), 'FATAL is set: stopping')
};

declare function trb:uris-start-set($value as xs:boolean)
 as empty-sequence()
{
  (: Ensure that the supplied value does not repeat,
   : by appending the minimum next unicode value.
   : This value almost certainly will not exist,
   : but even if it does the next cts:uris call
   : will produce the correct output.
   :)
  let $value := concat($value, '&#9;')
  let $do := xdmp:set-server-field('TRB-URIS-START', $value)
  let $do := xdmp:set($trb:URIS-START, $value)
  return ()
};

(: lib-trb.xqy :)
