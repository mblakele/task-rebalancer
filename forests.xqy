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

declare variable $LIMIT as xs:integer external ;

(: make sure uri lexicon is enabled :)
cts:uris((), 'limit=0'),
(: Look at local forests only.
 : Run on other hosts in the cluster to look at their forests.
 :)
let $local-forests := xdmp:host-forests(xdmp:host())
for $fid at $x in xdmp:database-forests(xdmp:database())
where $local-forests = $fid
return xdmp:spawn(
  'forest-uris.xqy',
  (xs:QName('FOREST'), $fid,
    xs:QName('INDEX'), $x,
    xs:QName('LIMIT'), $LIMIT))

(: forests.xqy :)
