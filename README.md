# Solr Grouping
### SOLR 5.5.3
Two level grouping in a solr search.

This code extends the single level grouping to two level grouping and fixes a couple of bugs.

We have a web archive collection containing web pages that has been built over several years, we have harvested the same sites at different times giving us a snapshots each time we harvest. When we search the collection we group the results into domains(sites) so a user can easily see and select pages from different domains. Because we have snapshots with in time this meant the the results were made up of the same page from different dates, so we needed to introduce a second level of grouping on URL so that the results would contain different pages.

When extending the SOLR grouping code I tried to keep the code generic so that it could possible used else where but I did not try to make all existing features work, only focusing on the parts that we needed for our system. Along the way I found a couple of bugs that I fixed in this code (1. Integer overflow in holding the total record count & 2. Not searching all shards when performing the second phase of the query(get all records within a group)). 

## Usage:
For the solr config
1. Place the Jar file in the server lib directory 

   *ie. server/solr-webapp/webapp/WEB-INF/lib*
   
1. Create a search component reference

   `<searchComponent name="NLADomainGroupingQueryComponent" class="org.apache.solr.handler.component.QueryComponentGrouping2" version="1" />`
   
1. Add the component to the request handler (Replaces the Query component)

   `<arr name="components">`
     `<str>NLADomainGroupingQueryComponent</str>`
     `</arr>`

When Quering use the std group commands **group=true** and **group.field=<field>**
For the second group field use the first group field to extend the command **group.field.<FirstField>=<SecondField>** _ie.group.field.site=url_

1. Add the grouping commands to the query
   `q=smith&group=true&group.field=site&group.field.site=url`
   
