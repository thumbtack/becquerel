# Connecting Becquerel to Salesforce

Read this for instructions on how to connect your Becquerel instance to Salesforce.

# Table of contents

[//]: # (Generate this with gd-md-toc: https://github.com/ekalinin/github-markdown-toc.go)
[//]: # (gh-md-toc salesforce.md | grep -v gh-md-toc | tail -n +5 | sed '/^$/d')

  * [Connecting Becquerel to Salesforce](#connecting-becquerel-to-salesforce)
  * [Table of contents](#table-of-contents)
  * [Prerequisites](#prerequisites)
  * [Create an external data source for Becquerel](#create-an-external-data-source-for-becquerel)
  * [Querying objects from Becquerel](#querying-objects-from-becquerel)
  * [Searching for objects from Becquerel](#searching-for-objects-from-becquerel)

# Prerequisites

Assumes you've already deployed Becquerel with Elasticsearch, the DVD Store data, and ngrok as in the [main `README`](../../README.md).

You need to be a Salesforce system administrator to configure external objects and data sources. If you're not one, go get [a Salesforce developer instance](https://developer.salesforce.com/signup) to practice on.

You should also at least skim the docs on [Salesforce Connect external data sources](https://help.salesforce.com/articleView?id=salesforce_connect_odata.htm).

# Create an external data source for Becquerel

First, let's tell Salesforce where to find Becquerel, and what data to retrieve from it.

Click the **Setup** link in the upper right of the Salesforce UI, under the gear menu.

![Setup location](salesforce-00-setup-location.png)

Click on **External Data Sources**.

![Setup external data sources](salesforce-01-setup-external-data-sources.png)

Fill in the settings as shown. Remember, your ngrok URL, username, and password will be different.

![Becquerel settings](salesforce-02-becquerel-settings.png)

On the next screen, click **Validate and Sync**. This will try to connect to Becquerel and fetch a list of tables.

![Validate and Sync button](salesforce-03-validate-and-sync-button.png)

This should show **Success** in the **Status** field, or you may have entered the wrong URL, Becquerel isn't running, Becquerel isn't reachable, etc. It should also show a list of tables available from Becquerel.

![Select tables](salesforce-04-select-tables.png)

Check all of the **Select** boxes for Becquerel's tables, then click the **Sync** button. This will create Salesforce external objects for each table, based on the table schema.

![Select tables checked](salesforce-05-select-tables-checked.png)

You should now see a list of external objects from Becquerel.

![Setup with external objects](salesforce-06-setup-with-external-objects.png)

Click **Edit** on one of the external object tables. We'll use `dvdstore__products` for this and further examples. You should see a list of fields and table metadata. At this point, we can search for objects in these tables in the Salesforce developer console, but we won't be able to see any of their fields in the actual Salesforce user UI, so we also need to edit the default page layout that was created on sync. Scroll down and click the **Edit** link for the page layout.

![External object example](salesforce-07-external-object-example.png)

Drag some fields into the **Information** or **System Information** sections, then click the **Save** button.

![External object layout example](salesforce-08-external-object-layout-example.png)

At this point, we're ready to look for objects from Becquerel tables.

# Querying objects from Becquerel

First, let's try a [SOQL](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql.htm) query through the [Salesforce developer console](https://developer.salesforce.com/page/Developer_Console).

Click the **Developer Console** link in the upper right of the Salesforce UI, under the gear menu.

![Developer console location](salesforce-09-dev-console-location.png)

The developer console will open in a new window.

![Developer console](salesforce-10-dev-console.png)

Click the **File** menu and then **Open**. This will show an entity browser for various parts of Salesforce configuration. You should be able to find the Becquerel external object tables at the end of the **Objects** list (since the example tables all use lower case names, and the browser sorts things in a case-sensitive way). Note that the Becquerel table names have been suffixed with `__x` by Salesforce. Select one of them and click the **Open** button.

![Object browser](salesforce-11-object-browser.png)

This should show the table schema. Note that all of the Becquerel column names have been suffixed with `__c` by Salesforce; the other ones are internal Salesforce columns and aren't stored in Becquerel. Click the **Query** button to bring up the SOQL query editor.

![Object schema](salesforce-12-object-schema.png)

Let's search for a DVD by title. Enter the following SOQL query and click the **Execute** button:

```sql
SELECT Id, id__c, title__c, actor__c
FROM dvdstore_products__x
WHERE title__c = 'AIRPORT POTLUCK'
```

![SOQL query results](salesforce-13-soql-query-results.png)

If you don't get any results, check for errors in Becquerel's console output, as well as ngrok's console output and web UI.

# Searching for objects from Becquerel

While SOQL is great, regular users will want to find things using a simple full-text query from the Salesforce search bar. Let's try to get that movie a different way.

Type `airport potluck` in the search bar and press **Return** or click the 🔍 icon.

![External object search](salesforce-14-external-object-search.png)

By default, external object search results don't show up in the top results tab, so scroll down a bit.

![External results](salesforce-15-external-results.png)

The **External Results** section should have a tab for each Becquerel table. Click on the one for `dvdstore__products`, then click the **External ID** for the first result.

![External object found](salesforce-16-external-object-found.png)

And there it is. Click on the **Details** tab to see the page layout we set up earlier.

![External object details](salesforce-17-external-object-details.png)

