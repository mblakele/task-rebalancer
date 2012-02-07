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

declare function local:maybe-spawn($uri as xs:string, $assignment as xs:integer)
  as xs:boolean?
{
  (: is the document already where it ought to be? :)
  if ($assignment eq $INDEX) then ()
  else try {
    true(),
    xdmp:spawn(
      'rebalance.xqy',
      (xs:QName('URI'), $uri,
        xs:QName('ASSIGNMENT'), subsequence($FORESTS, $assignment, 1))) }
  catch ($ex) {
    if ($ex/error:code eq 'XDMP-MAXTASKS') then xdmp:set($IS-MAXTASKS, true())
    else xdmp:rethrow() }
};

xdmp:log(
  text {
    'forest-uris.xqy: forest', xdmp:forest-name($FOREST), 'limit', $LIMIT })
,
xdmp:log(
  text {
    'forest-uris.xqy: forest', xdmp:forest-name($FOREST), 'limit', $LIMIT,
    'spawned', $SPAWN-COUNT })

(: forest-uris.xqy :)
