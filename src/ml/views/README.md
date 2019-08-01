# Locus ML View Develop Guide

In order to create a new type of view, we need to:
* Create a new file with the name `<view_type>.js` under `/src/ml/views`
* Implement the view file following the **interface** below
* In file `/src/ml/view/index.js`, Append `view_type` into `VIEW_LIST`
* The view can be seen and used through Locus ML now

After creating the view, please update the view document at: https://github.com/EQWorks/firstorder/wiki/Locus-ML-View

## Interface
A view file requires two functions: `listViews()` and `getView()`

See `views/report.js` and `views/ext.js` for example

### listViews(access, filter)
This function accepts a user access object and an optional filter object, and should return an array of user accessible view metadata objects

The process of function:
* Using the provided access and filter, get a list of accessible views(most of the time by querying sql table)
* Process and format each view object into the **standrad format below**

#### view id object
the view id object identifies a view, and consist of a few required fields and some optional fields:

* type: [required] view type, same as file name
* `<view specific id>`: can have one or more this type of fields, used for query the underlaying tables for a view. like: `reportID` and `layerID` for report or `connectionID` for external data
* id: [required] view id string, uniquely identifies a view, must be string concatenation of view type and all view type specific ids.

example:
```
{
  "type": "report",
  "report_id": 981,
  "layer_id": 457,
  "id": "report_457_981"
}
```

#### view object (standrad format)
The standard format of a view meta object is:
```
{
  // required:
  "name": "walmart canada", // view display name
  "view": { ... }           // the view id object
  columns: [
    "poi_id": {               // key is column name
      "category": CAT_NUMERIC // category should be one of the categories in `src/ml/type.js`, starts with 'CAT_'
    },
    ...
  ],

  // and some other view specific meta data for front end, like:
  layer_type_id: 2,
  report_type: 1,
}
```

### getView(access, reqViews, reqViewColumns, viewIDObject)
* access: user access object
* reqViews: an object under request, we need to inject the end result view into this object
* reqViewColumns: an object under request, need to inject the view columns into it
* viewIDObject: the view id object, but excluding `type` and `id`, contains only view specific ids


The process of function:
* Create the `viewID` constant by concatenating view type and other view specific id
* If necessary, validate access to view using `access` (can be combined with the next step)
* Using `listViews(access, filter)` with **filter** to get the columns for this view, and inject into `reqViewColumns` using `viewID` as key
* Use `knex.raw()` to query the necessary tables with provided view specific ids, be sured to wrap the whole query and give it an alias name `viewID`, like: `` knex.raw(`(<query>) as ${viewID}`) ``
* Assign the result from `knex.raw()` to `reqViews` using `viewID` as key
