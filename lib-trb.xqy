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

(: TODO rescheduling mechanism to prioritize largest forests? :)

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

declare variable $TASKS-COUNT := 0 ;

declare variable $URI-LAST := () ;

declare function trb:fatal-set($value as xs:boolean)
 as empty-sequence()
{
  xdmp:set-server-field('com.blakeley.task-rebalancer.FATAL', $value)[0],
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
  (: The submitted value failed due to MAXTASKS or the limit was reached.
   : Store it so that the next task on that forest can resume.
   : Must use a server field so scheduled tasks can persist the value.
   : Take care to avoid returning the value.
   :)
  xdmp:set-server-field(trb:uris-start-name($forest), $value)[0]
};

declare function trb:spawn-again(
  $module as xs:string,
  $forest as xs:unsignedLong,
  $index as xs:integer,
  $forest-name as xs:string,
  $limit as xs:integer,
  $millis as xs:integer)
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
    xdmp:log(
      text {
        concat($module, ':'), 'respawned', $forest-name,
        'after', $millis, 'ms',
        'with', $forest, $index, true(), $limit },
      'debug') }
  catch ($ex) {
    if ($ex/error:code ne 'XDMP-MAXTASKS') then xdmp:rethrow()
    else trb:spawn-again(
      $module, $forest, $index, $forest-name, $limit,
      (: back off before next retry :)
      2 * $millis) }
};

declare function trb:maybe-spawn2(
  $forest-name as xs:string,
  $uri as xs:string,
  $index as xs:integer,
  $forests as xs:unsignedLong+,
  $assignment as xs:integer)
{
  xdmp:log(
    text {
      'trb:maybe-spawn2:', $forest-name, $uri, $index, $assignment },
    'fine'),
  (: fail as quickly as possible :)
  trb:maybe-fatal(),
  (: Local to this module, keep track of the last uri checked. :)
  xdmp:set($URI-LAST, $uri),
  (: is the document already where it ought to be? :)
  if ($assignment eq $index) then () else (
    xdmp:spawn(
      'rebalance.xqy',
      (xs:QName('URI'), $uri,
        xs:QName('ASSIGNMENT'), subsequence($forests, $assignment, 1))),
    (: Increment the task count. :)
    xdmp:set($TASKS-COUNT, 1 + $TASKS-COUNT),
    (: give any competing threads a chance :)
    xdmp:sleep(1) )
};

(: use a function to avoid FLWOR, for result streaming :)
declare function trb:maybe-spawn(
  $forest-name as xs:string,
  $uris-start as xs:string?,
  $uri as xs:string,
  $index as xs:integer,
  $forests as xs:unsignedLong*)
{
  (: It is tricky to advance the starting point,
   : so we expect that the first URI of this batch
   : was the last uri of the last batch.
   :)
  if ($uri eq $uris-start) then ()
  else (
    xdmp:log(
      text {
        'trb:maybe-spawn:', $forest-name, $uri, $index },
      'fine'),
    trb:maybe-spawn2(
      $forest-name, $uri, $index,
      $forests, xdmp:document-assign($uri, count($forests))))
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
      if (not($uris-start)) then () else 'starting from', $uris-start },
    'info'),
  (: Use function mapping to avoid FLWOR, for streaming. :)
  trb:maybe-spawn(
    $forest-name, $uris-start,
    cts:uris(
      (: If this forest was respawned due to maxtasks,
       : this ensures that we pick up where we left off.
       : In combination with the lock-for-update call in this module,
       : it also protects against multiple tasks running on the same forest.
       :
       : TODO Support properties, directories, etc.
       :)
      $uris-start,
      ('document',
        if ($limit lt 1) then () else concat('limit=', $limit)),
      (), (), $forest),
    $index, $forests)
};

declare function trb:spawn-preflight(
  $forest as xs:unsignedLong,
  $forest-status as element())
{
  (: preflight :)
  trb:maybe-fatal(),
  (: Grabbing the URI lock acts like a semaphore, to keep other tasks out.
   : This does not prevent multiple forest-uris tasks from respawning,
   : but it does minimize the resulting havoc.
   : Elsewhere, URIS-START acts as another guard against extra rebalancing work,
   : as do the document locks themselves.
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
};

declare function trb:spawn-postflight(
  $module as xs:string,
  $forest as xs:unsignedLong,
  $index as xs:integer,
  $forest-name as xs:string,
  $respawn as xs:boolean,
  $limit as xs:integer,
  $is-maxtasks as xs:boolean,
  $tasks-count as xs:integer,
  $uri-last as xs:string?)
{
  if (not($is-maxtasks)) then ()
  else (
    xdmp:log(
      text {
        'trb:spawn-postflight:', xdmp:forest-name($forest),
        'task server queue limit reached,',
        if ($respawn) then 'will respawn' else 'will not respawn' },
      'debug'),
    if (not($respawn)) then () else trb:spawn-again(
      $module, $forest, $index, xdmp:forest-name($forest), $limit,
      (: initial sleep, to allow processing of queued tasks :)
      4 * 1000)),

  (: log the final count :)
  xdmp:log(
    text {
      'trb:spawn-postflight:', $forest-name, 'limit', $limit,
      'spawned', $tasks-count,
      'start', xdmp:describe(trb:uris-start($forest)),
      'last', xdmp:describe($uri-last),
      'maxtasks', $is-maxtasks,
      if ($is-maxtasks and $respawn) then '(will respawn)'
      else '(no respawn)' }),
  (: If we hit the task limit, the uris-start state has already been set.
   : Otherwise we set the host state to URI-LAST.
   : This will automatically track the starting point
   : for the next run, whether it is a scheduled task or a respawn.
   : This lets $limit act like a batch size for scheduled tasks,
   : and a tuning parameter for respawn.
   :)
  trb:uris-start-set(
    $forest,
    (: This has the effect of resetting any loops. :)
    if (trb:uris-start($forest) eq $uri-last) then () else $uri-last)
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
  trb:spawn-preflight($forest, $forest-status),
  trb:maybe-fatal(),

  try {
    trb:spawn(
      $module, $forest, $index, $forest-status/fs:forest-name,
      trb:uris-start($forest), $forests, $respawn, $limit),
    trb:spawn-postflight(
      $module, $forest, $index,
      $forest-status/fs:forest-name, $respawn, $limit,
      false(), $TASKS-COUNT, $URI-LAST) }
  catch ($ex) {
    if (not($ex/error:code = 'XDMP-MAXTASKS')) then xdmp:rethrow()
    else trb:spawn-postflight(
      $module, $forest, $index,
      $forest-status/fs:forest-name, $respawn, $limit,
      true(), $TASKS-COUNT, $URI-LAST) }
};

declare function trb:forests-map()
as map:map
{
  (: Look at local forests only, using a map of id to index.
   : Run on other hosts in the cluster to look at their forests.
   :)
  let $m := map:map()
  let $do := (
    let $local-forests := xdmp:host-forests(xdmp:host())
    for $fid at $x in xdmp:database-forests(xdmp:database())
    where $local-forests = $fid
    return map:put($m, string($fid), $x))
  return $m
};

(: lib-trb.xqy :)
