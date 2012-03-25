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

declare namespace fs="http://marklogic.com/xdmp/status/forest" ;

(: This code is designed to minimize FLWOR expressions,
 : and maximize streaming.
 :)

(: We need a bail-out mechanism to stop the respawns.
 : This variable acts as a kill signal.
 :)
declare variable $FATAL := xdmp:get-server-field(
  'com.blakeley.task-rebalancer.FATAL') ;

declare variable $IS-MAXTASKS := false() ;

declare function trb:fatal-set($value as xs:boolean)
 as empty-sequence()
{
  xdmp:set-server-field('com.blakeley.task-rebalancer.FATAL', $value),
  xdmp:set($trb:FATAL, $value)
};

declare function trb:maybe-fatal()
as empty-sequence()
{
  if (not($trb:FATAL)) then ()
  else error((), 'FATAL is set: stopping')
};

declare private function trb:uris-start-name(
  $forest as xs:unsignedLong)
as xs:string
{
  concat('com.blakeley.task-rebalancer.URIS-START/', $forest)
};

(: get uris-start state :)
declare function trb:uris-start(
  $forest as xs:unsignedLong)
as xs:string?
{
  xdmp:get-server-field(trb:uris-start-name($forest))
};

(: clear or set value of the uris-start state :)
declare function trb:uris-start-set(
  $forest as xs:unsignedLong,
  $value as xs:string?)
 as empty-sequence()
{
  (: The submitted value failed due to MAXTASKS.
   : Store it so that the next task on that forest can resume.
   : Do not return the value.
   :)
  xdmp:set-server-field(trb:uris-start-name($forest), $value)[0]
};

declare function trb:spawn-again(
  $uri as xs:string,
  $module as xs:string,
  $forest as xs:unsignedLong,
  $index as xs:integer,
  $forest-name as xs:string,
  $limit as xs:integer,
  $millis as xs:integer)
as empty-sequence()
{
  (: fail as quickly as possible :)
  trb:maybe-fatal(),
  xdmp:sleep($millis),
  (: fail as quickly as possible :)
  trb:maybe-fatal(),
  xdmp:log(
    text { concat($module, ':'), 'trying respawn', $forest-name, $millis },
    'fine'),
  try {
    xdmp:spawn(
      $module,
      (xs:QName('FOREST'), $forest,
        xs:QName('INDEX'), $index,
        xs:QName('RESPAWN'), true(),
        xs:QName('LIMIT'), $limit)),
    trb:uris-start-set($forest, $uri),
    xdmp:log(
      text { 'forest-uris.xqy:', $forest-name, 'respawn ok' },
      'debug') }
  catch ($ex) {
    if ($ex/error:code ne 'XDMP-MAXTASKS') then xdmp:rethrow()
    else trb:spawn-again(
      $uri, $module, $forest, $index, $forest-name, $limit,
      (: back off before next retry :)
      2 * $millis) }
};

(: NB - forests-count is for arity overloading only :)
declare function trb:maybe-spawn(
  $uri as xs:string,
  $assignment as xs:integer,
  $module as xs:string,
  $forest as xs:unsignedLong,
  $index as xs:integer,
  $forest-name as xs:string,
  $forests as xs:unsignedLong*,
  $forests-count as xs:integer,
  $respawn as xs:boolean,
  $limit as xs:integer)
as xs:boolean?
{
  (: fail as quickly as possible :)
  trb:maybe-fatal(),
  (: is the document already where it ought to be? :)
  if ($assignment eq $index) then ()
  else try {
    true(),
    xdmp:spawn(
      'rebalance.xqy',
      (xs:QName('URI'), $uri,
        xs:QName('ASSIGNMENT'), subsequence($forests, $assignment, 1))),
    (: give any competing threads a chance :)
    xdmp:sleep(1) }
  catch ($ex) {
    if ($ex/error:code eq 'XDMP-MAXTASKS') then () else xdmp:rethrow(),
    xdmp:log(
      text {
        'forest-uris.xqy:', $forest-name, 'task server queue limit reached,',
        if ($respawn) then 'will respawn' else 'will not respawn' },
      'debug'),
    xdmp:set($IS-MAXTASKS, true()),
    if (not($respawn)) then ()
    else trb:spawn-again(
      $uri,
      $module, $forest, $index, $forest-name, $limit,
      (: initial sleep, to allow processing of queued tasks :)
      4 * 1000) }
};

(: use a function to avoid FLWOR, for result streaming :)
declare function trb:maybe-spawn(
  $uri as xs:string,
  $module as xs:string,
  $forest as xs:unsignedLong,
  $index as xs:integer,
  $forest-name as xs:string,
  $forests as xs:unsignedLong*,
  $forests-count as xs:integer,
  $respawn as xs:boolean,
  $limit as xs:integer)
as xs:boolean?
{
  if ($IS-MAXTASKS) then ()
  else trb:maybe-spawn(
    $uri, xdmp:document-assign($uri, $forests-count),
    $module, $forest, $index, $forest-name,
    $forests, $forests-count, $respawn, $limit)
};

declare function trb:spawn(
  $module as xs:string,
  $forest as xs:unsignedLong,
  $index as xs:integer,
  $forest-name as xs:string,
  $uris-start as xs:string?,
  $forests as xs:unsignedLong*,
  $respawn as xs:boolean,
  $limit as xs:integer)
{
  xdmp:log(
    text {
      concat($module, ':'), $forest-name, 'limit', $limit,
      if (not($uris-start)) then () else 'starting from', $uris-start }),
  (: NB - function mapping to avoid FLWOR :)
  trb:maybe-spawn(
    cts:uris(
      (: If this forest was respawned due to maxtasks,
       : this ensures that we pick up where we left off.
       : In combination with the lock-for-update call in this module,
       : it also protects against multiple tasks running on the same forest.
       :)
      $uris-start, (
        'document',
        if ($limit lt 1) then () else (
          for $i in ('limit', 'sample', 'truncate')
          return concat($i, '=', $limit))),
      (: fodder for the sample and truncate options :)
      cts:and-query(()),
      (),
      $forest),
    $module, $forest, $index, $forest-name, $forests,
    count($forests), $respawn, $limit)
};

declare function trb:spawn(
  $module as xs:string,
  $forest as xs:unsignedLong,
  $index as xs:integer,
  $forest-status as element(),
  $forests as xs:unsignedLong*,
  $respawn as xs:boolean,
  $limit as xs:integer)
{
  (: preflight :)
  trb:maybe-fatal(),
  (: Grabbing the URI lock acts like a semaphore, to keep other tasks out.
   : This does not prevent multiple forest-uris tasks from respawning,
   : but it does minimize the resulting havoc.
   : Elsewhere, URIS-START acts as another guard against extra rebalancing work.
   :)
  xdmp:lock-for-update(concat('com.blakeley.task-rebalancer/', $forest)),
  (: Make sure we have not suffered a forest failover event.
   : NB - this does not protect against failover events after tasks are queued.
   :)
  if ($forest-status/fs:host-id eq xdmp:host()) then () else error(
    (), 'TRB-NOTLOCAL',
    text {
      $forest-status/fs:forest-name, 'is not local to',
      xdmp:host-name(xdmp:host())})
  ,
  if ($forest-status/fs:current-master-forest eq $forest) then () else error(
    (), 'TRB-NOTLOCAL',
    text {
      $forest-status/fs:forest-name, 'master is not local to',
      xdmp:host-name(xdmp:host())})
  ,
  trb:maybe-fatal(),
  (: spawn the tasks and log the final count :)
  xdmp:log(
    text {
      concat($module, ':'), $forest-status/fs:forest-name, 'limit', $limit,
      'spawned',
      count(
        trb:spawn(
          $module, $forest, $index, $forest-status/fs:forest-name,
          trb:uris-start($forest), $forests, $respawn, $limit)),
      if ($IS-MAXTASKS) then 'will respawn' else 'done' }),
  (: Did we queue all tasks? If so, clear the start position :)
  if ($IS-MAXTASKS) then () else trb:uris-start-set($forest, ())
};

(: lib-trb.xqy :)
