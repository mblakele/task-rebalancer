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

declare variable $ASSIGNMENT as xs:unsignedLong external ;

declare variable $URI as xs:string external ;

(: update the existing document, by overwriting it :)
xdmp:document-insert(
  $URI,
  doc($URI),
  xdmp:document-get-permissions($URI),
  xdmp:document-get-collections($URI),
  xdmp:document-get-quality($URI),
  (: explicitly place the document in the correct forest :)
  $ASSIGNMENT)

(: rebalance.xqy :)
