# cubes-mongo-gridfs

This module implement a mongodb gridFS backend for python Cubes OLAP Framework 1.0.1. (https://github.com/DataBrewery/cubes).

The idea is to store measures about 'facts' inside multi spectral tif files and use the cubes OLAP functionalities to query the data.

Actually just the aggregation queries has been tested.

Starting from a mongogridfs bucket the files collection must contain dimension to perform Cuts.

Then the model cubes aggregates contains a list like

`
{
  "name": "record_count",
  "function": "count"
},
{
  "name": "band one sum",
  "function": "sum",
  "measure": "1"
}
`  
and inside the database the uploaded tif file can have whatever number of bands.

Actually to browse aggreagtes is mandatory to pass the aggregates option whit record_count and the name of one or more measures to aggregate (actually the aggregation perform only sums) 

`result = browser.aggregate(aggregates=["record_count", "band one sum"])`

Then we obtain a result.summary like

`
[{u'_id': {},
  u'record_count': 11187,
  u'band one sum': array([[  72.81678009,   74.87879181,   76.14283752, ...,  257.91091919,
         167.71795654,  148.89985657],
       [  71.65176392,   76.10292816,   76.14283752, ...,  206.05926514,
         172.94056702,  158.99935913],
       [  67.85961914,   76.14283752,   72.84747314, ...,  236.62388611,
         280.89239502,  156.49520874],
       ..., 
       [  78.43511963,   76.63755035,   79.03015137, ...,   53.65074539,
          53.95258713,   57.38922501],
       [  77.97314453,   78.16474152,   79.02905273, ...,   53.65074539,
          53.65074539,   53.67944717],
       [  76.96592712,   77.96524811,   79.02905273, ...,   53.61225128,
          53.65074539,   53.44342422]], dtype=float32)]
`
