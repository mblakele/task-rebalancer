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
declare namespace hs="http://marklogic.com/xdmp/status/host" ;
declare namespace ss="http://marklogic.com/xdmp/status/server" ;

declare variable $LIMIT as xs:integer external ;

declare variable $RESPAWN as xs:boolean external ;

declare variable $FORESTS-MAP := (
  (: Look at local forests only, using a map of index to id.
   : Run on other hosts in the cluster to look at their forests.
   :)
  let $m := map:map()
  let $do := (
    let $local-forests := xdmp:host-forests(xdmp:host())
    for $fid at $x in xdmp:database-forests(xdmp:database())
    where $local-forests = $fid
    return map:put($m, string($x), $fid))
  return $m );

(: Make sure uri lexicon is enabled. :)
cts:uris((), 'limit=0'),
(: NB - cannot check TRB-FATAL because it is set on the task server :)

(: Make sure we have at least one task server thread per local forest.
 : This prevents forest-uris respawning from deadlocking the task server.
 :)
let $host := xdmp:host()
let $tid := xdmp:host-status($host)/hs:task-server/hs:task-server-id
let $threads := xdmp:server-status($host, $tid)/ss:max-threads/data(.)
let $assert := (
  if (not($RESPAWN)) then ()
  else if (count(map:keys($FORESTS-MAP)) lt $threads) then ()
  else error(
    (), 'TRB-TOOFEWTHREADS',
    text {
      'to avoid deadlocks,',
      'configure the task server with at least',
      1 + count(map:keys($FORESTS-MAP)), 'threads' }))
for $key in map:keys($FORESTS-MAP)
let $fid := map:get($FORESTS-MAP, $key)
(: give larger forests priority :)
order by xdmp:estimate(
  cts:search(doc(), cts:and-query(()), (), (), $fid)) descending
return xdmp:spawn(
  'forest-uris.xqy',
  (xs:QName('FOREST'), $fid,
    xs:QName('INDEX'), xs:integer($key),
    xs:QName('RESPAWN'), $RESPAWN,
    xs:QName('LIMIT'), $LIMIT))

(: forests.xqy :)
