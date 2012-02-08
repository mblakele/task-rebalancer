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

declare namespace fs="http://marklogic.com/xdmp/status/forest";

(: This code is designed to minimize FLWOR expressions,
 : and maximize streaming.
 :)

(: the forest to rebalance :)
declare variable $FOREST as xs:unsignedLong external ;

declare variable $INDEX as xs:integer external ;

declare variable $LIMIT as xs:integer external ;

declare variable $IS-MAXTASKS := false() ;

declare variable $FOREST-NAME := xdmp:forest-name($FOREST) ;

declare variable $FORESTS := xdmp:database-forests(xdmp:database()) ;

declare variable $COUNT := count($FORESTS) ;

declare variable $SPAWN-COUNT := count(
  local:maybe-spawn(
    cts:uris(
      (), (
        'document',
        if ($LIMIT lt 1) then () else (
          for $i in ('limit', 'sample', 'truncate')
          return concat($i, '=', $LIMIT))),
      (: fodder for the sample and truncate options :)
      cts:and-query(()),
      (),
      $FOREST))) ;

(: use a local function to avoid FLWOR, for result streaming :)
declare function local:maybe-spawn($uri as xs:string)
as xs:boolean?
{
  if ($IS-MAXTASKS) then ()
  else local:maybe-spawn($uri, xdmp:document-assign($uri, $COUNT))
};

declare function local:spawn-again($millis as xs:integer)
  as empty-sequence()
{
  (: fail as quickly as possible :)
  if (not(xdmp:get-server-field('TRB-FATAL'))) then ()
  else error((), 'TRB-FATAL is set'),
  xdmp:sleep($millis),
  (: fail as quickly as possible :)
  if (not(xdmp:get-server-field('TRB-FATAL'))) then ()
  else error((), 'TRB-FATAL is set'),
  xdmp:log(
    text { 'trying respawn', $FOREST-NAME, $millis }, 'debug'),
  try {
    xdmp:spawn(
      'forest-uris.xqy',
      (xs:QName('FOREST'), $FOREST,
        xs:QName('INDEX'), $INDEX,
        xs:QName('LIMIT'), $LIMIT)),
    xdmp:log(
      text { 'respawn ok for', $FOREST-NAME, $millis }, 'debug') }
  catch ($ex) {
    if ($ex/error:code ne 'XDMP-MAXTASKS') then xdmp:rethrow()
    else local:spawn-again(2 * $millis) }
};

declare function local:maybe-spawn($uri as xs:string, $assignment as xs:integer)
  as xs:boolean?
{
  if (not(xdmp:get-server-field('TRB-FATAL'))) then ()
  else error((), 'TRB-FATAL is set')
  ,
  (: is the document already where it ought to be? :)
  if ($assignment eq $INDEX) then ()
  else try {
    true(),
    xdmp:spawn(
      'rebalance.xqy',
      (xs:QName('URI'), $uri,
        xs:QName('ASSIGNMENT'), subsequence($FORESTS, $assignment, 1))) }
  catch ($ex) {
    if ($ex/error:code ne 'XDMP-MAXTASKS') then xdmp:rethrow()
    else (
      xdmp:log(
        text { 'task server queue limit reached', $FOREST-NAME }, 'info'),
      xdmp:set($IS-MAXTASKS, true()),
      (: make an effort to respawn :)
      local:spawn-again(4 * 1000)) }
};

xdmp:log(
  text {
    'forest-uris.xqy:', $FOREST-NAME, 'limit', $LIMIT })
,
(: We need a bail-out mechanism to stop the respawns.
 : This document acts as a kill signal.
 :)
if (not(xdmp:get-server-field('TRB-FATAL'))) then ()
else error((), 'TRB-FATAL is set')
,
(: try to make the race for queue space a little fairer :)
let $millis := xdmp:random(5000)
return xdmp:log(
  text { 'forest-uris.xqy:', $FOREST-NAME, 'sleeping', $millis, 'ms' })
,
(: looking at the value of SPAWN-COUNT will spawn the tasks :)
xdmp:log(
  text {
    'forest-uris.xqy:', $FOREST-NAME, 'limit', $LIMIT,
    'spawned', $SPAWN-COUNT })

(: forest-uris.xqy :)
