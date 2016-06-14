from ...logging import get_logger
from ...errors import *
from ...browser import *
from ...computation import *
from ...statutils import calculators_for_aggregates, available_calculators
from cubes import statutils
from .mapper import MongoCollectionMapper
from .datesupport import MongoDateSupport
from .functions import get_aggregate_function, available_aggregate_functions
from .util import to_json_safe, collapse_record

import collections
import copy
import pymongo
from bson.objectid import ObjectId # added for query mongo elements using ObjectId
import gridfs
import gdal
import ogr
import osr
import bson
import re

from dateutil.relativedelta import relativedelta
from datetime import datetime
from itertools import groupby
from functools import partial
import pytz


tz_utc = pytz.timezone('UTC')


SO_FAR_DIMENSION_REGEX = re.compile(r"^.+_sf$", re.IGNORECASE)


def is_date_dimension(dim):
    if hasattr(dim, 'role') and (dim.role == 'time'):
        return True
    if hasattr(dim, 'info') and (dim.info.get('is_date')):
        return True
    return False


class MongogfsBrowser(AggregationBrowser):
    def __init__(self, cube, store, locale=None, calendar=None,
                 **options):

        super(MongogfsBrowser, self).__init__(cube, store)

        self.logger = get_logger()

        database = store.database
        if cube.browser_options.get('database'):
            database = cube.browser_options.get('database')

        collection = store.collection
        if cube.browser_options.get('collection'):
            collection = cube.browser_options.get('collection')

        # Added bucket name option
        bucket = store.bucket
        if cube.browser_options.get('bucket'):
            bucket = cube.browser_options.get('bucket')

        fullcollectionname = bucket+'.'+collection
        self.data_store = store.client[database][fullcollectionname]

        self.fs = gridfs.GridFS(store.client[database], collection=bucket)

        self.fsbucket = gridfs.GridFSBucket(store.client[database], bucket_name=bucket)

        self.mapper = MongoCollectionMapper(cube, database, collection, locale)

        self.timezone = pytz.timezone(cube.browser_options.get('timezone') or options.get('timezone') or 'UTC')

        self.datesupport = MongoDateSupport(self.logger, calendar)

        # if "__query__" in self.cube.mappings:
        #     self.logger.warn("mongo: __query__ in mappings is depreciated, "
        #                      "use browser_options.filter instead")

        self.query_filter = options.get("filter", None)

    def features(self):
        """Return MONGO features."""

        features = {
            "facts": ["fields", "missing_values"],
            "aggregate_functions": available_aggregate_functions(),
            "post_aggregate_functions": available_calculators()
        }

        cube_actions = self.cube.browser_options.get("actions")

        default_actions = ["aggregate", "members", "fact", "facts", "cell"]
        cube_actions = self.cube.browser_options.get("actions")

        if cube_actions:
            cube_actions = set(default_actions) & set(cube_actions)
            features["actions"] = list(cube_actions)
        else:
            features["actions"] = default_actions

        return features

    def set_locale(self, locale):
        self.mapper.set_locale(locale)

    def provide_aggregate(self, cell, aggregates, drilldown, split, order,
                          page, page_size, **options):

        result = AggregationResult(cell=cell, aggregates=aggregates)

        drilldown_levels = None

        labels = []

        # Prepare the drilldown
        # FIXME: this is the exact code as in SQL browser - put it into a
        # separate method and share

        if drilldown or split:
            if not (page_size and page is not None):
                self.assert_low_cardinality(cell, drilldown)

            result.levels = drilldown.result_levels(include_split=bool(split))

            #
            # Find post-aggregation calculations and decorate the result
            #
            result.calculators = calculators_for_aggregates(self.cube,
                                                            aggregates,
                                                            drilldown,
                                                            split,
                                                            available_aggregate_functions())

        ## Summary and Items
        ## -----------------
        summary, items = self._do_aggregation_query_gfs(cell=cell,
                                                    aggregates=aggregates,
                                                    attributes=None,
                                                    drilldown=drilldown,
                                                    split=split, order=order,
                                                    page=page,
                                                    page_size=page_size)

        result.cells = iter(items)
        result.summary = summary or {}

        # add calculated measures w/o drilldown or split if no drilldown or split
        if not (drilldown or split):
            calculators = calculators_for_aggregates(self.cube,
                                                     aggregates,
                                                     drilldown,
                                                     split,
                                                     available_aggregate_functions())
            for calc in calculators:
                calc(result.summary)

        labels += [ str(m) for m in aggregates ]
        result.labels = labels

        return result

    def is_builtin_function(self, function_name, aggregate):
        return function_name in available_aggregate_functions()

    def facts(self, cell=None, fields=None, order=None, page=None, page_size=None,
              **options):
        """Return facts iterator."""

        cell = cell or Cell(self.cube)

        if not fields:
            attributes = self.cube.all_attributes
            self.logger.debug("facts: getting all fields")
        else:
            attributes = self.cube.get_attributes(fields)
            self.logger.debug("facts: getting fields: %s" % fields)

        # Prepare the query
        query_obj, fields_obj = self._build_query_and_fields(cell, [], for_project=False)

        # TODO include fields_obj, fully populated
        cursor = self.data_store.find(query_obj)

        order = self.prepare_order(order)
        if order:
            order_obj = self._order_to_sort_object(order)
            k, v = order_obj.iteritems().next()
            cursor = cursor.sort(k, pymongo.DESCENDING if v == -1 else pymongo.ASCENDING)

        if page_size and page > 0:
            cursor = cursor.skip(page * page_size)

        if page_size and page_size > 0:
            cursor = cursor.limit(page_size)

        facts = MongoFactsIterator(cursor, attributes, self.mapper,
                                   self.datesupport)

        return facts

    def fact(self, key):
        # TODO make it possible to have a fact key that is not an ObjectId
        key_field = self.mapper.physical(self.mapper.attribute(self.cube.key))
        key_value = key
        try:
            key_value = bson.objectid.ObjectId(key)
        except:
            pass
        item = self.data_store.find_one({key_field.field: key_value})
        if item is not None:
            item = to_json_safe(item)
        return item

    def provide_members(self, cell, dimension, depth=None, hierarchy=None,
                        levels=None, attributes=None, page=None,
                        page_size=None, order=None):
        """Provide dimension members. The arguments are already prepared by
        superclass `members()` method."""

        attributes = []
        for level in levels:
           attributes += level.attributes

        drilldown = Drilldown([(dimension, hierarchy, levels[-1])], cell)

        summary, cursor = self._do_aggregation_query(cell=cell,
                                                     aggregates=None,
                                                     attributes=attributes,
                                                     drilldown=drilldown,
                                                     split=None,
                                                     order=order,
                                                     page=page,
                                                     page_size=page_size)

        # TODO: return iterator
        data = []

        for item in cursor:
            new_item = {}
            for level in levels:
                for level_attr in level.attributes:
                    k = level_attr.ref()
                    if item.has_key(k):
                        new_item[k] = item[k]
            data.append(new_item)

        return data

    def _in_same_collection(self, physical_ref):
        return (physical_ref.database == self.mapper.database) and (physical_ref.collection == self.mapper.collection)


    def _build_query_and_fields_gfs(self, cell, attributes, for_project=False):

        find_clauses = []
        query = {}

        ids = []
        for cut in cell.cuts:
            find_clauses += self._query_conditions_for_cut(cut, for_project)

        if find_clauses:
            query.update({"$and": find_clauses})

        for id_list in self.data_store.find(query) :
            ids.append(id_list['_id'])

        return query, ids

    def _do_aggregation_query_gfs(self, cell, aggregates, attributes, drilldown,
                              split, order, page, page_size):

        # From the cell element
        # the _build_query_and_fields_gfs must return :
        # query_obj -- to calculate the record count
        # fileids_list -- that contain the file object ids for querying gridfs
        query_obj, fileids_list = self._build_query_and_fields_gfs(cell, attributes)

        # pipeline for aggregate request to mongodb to obtain back a record count
        pipeline = []
        pipeline.append({ "$match": query_obj })
        pipeline.append({ "$project": {'record_count': '$record_count'} })
        pipeline.append({ "$group": {'record_count': {'$sum': 1}, '_id': {}} })

        results = self.data_store.aggregate(pipeline) # aggregate respect to mongodb
        result_agg = []
        for record_count in results:
            result_items = record_count
        band_id = []
        for item in aggregates :
            if item.measure is not None:
                dicto = {'band':int(item.measure),'name':item.name, 'function':item.function}
                band_id.append(dicto)

        for bands in band_id :
            count = 0
            for objectid in fileids_list:
                grid_out = self.fs.get(objectid)
                # # Use a virtual memory file, which is named like this
                vsipath = '/vsimem/from_mongodb'
                gdal.FileFromMemBuffer(vsipath, grid_out.read())
                ds = gdal.Open(vsipath)
                # # get the band data array
                band = ds.GetRasterBand(bands['band'])
                arr = band.ReadAsArray()
                if count == 0 :
                     agg_array = arr
                     count = 1
                else :
                     agg_array += arr

                # # Close and clean up virtual memory file
                ds = band = None
                gdal.Unlink(vsipath)

            result_items[bands['name']] = agg_array

        result_agg.append(result_items)
        return (result_agg, [])

    def _build_date_for_cut(self, hier, path, is_end=False):
        """Constructs a date from timestamp."""
        date_dict = {'month': 1, 'day': 1, 'hour': 0, 'minute': 0 }
        min_part = None

        date_levels = hier.levels[:len(path)]
        for val, date_part in zip(path, date_levels):
            physical = self.mapper.physical(date_part.key)
            date_dict[date_part.key.name] = physical.convert_value(val)
            min_part = date_part.key.name

        dt = None
        if 'year' in date_dict:
            dt = datetime(**date_dict)
            if is_end:
                dt += relativedelta(**{(min_part + 's'): 1})
        else:
            if 'week' not in date_dict:
                return None
            else:
                dt = datetime.strptime(date_dict['week'], '%Y-%m-%d')
                if is_end:
                    dt = self.datesupport.get_week_end_date(dt) + relativedelta(days=1)
                else:
                    dt = self.datesupport.get_week_start_date(dt)

        return self.timezone.localize(dt).astimezone(tz_utc)

    def _query_conditions_for_cut(self, cut, for_project=False):

        conds = []
        cut_dimension = self.cube.dimension(cut.dimension)
        cut_hierarchy = cut_dimension.hierarchy(cut.hierarchy)

        if isinstance(cut, PointCut):
            if is_date_dimension(cut.dimension):
                start = self._build_date_for_cut(cut_hierarchy, cut.path)
                if start is None:
                    return conds

                end = self._build_date_for_cut(cut_hierarchy, cut.path, is_end=True)

                if not cut.invert:
                    start_op = '$gte'
                    end_op = '$lt'
                else:
                    start_op = '$lt'
                    end_op = '$gt'

                key = cut_hierarchy.levels[0].key

                start_cond = self._query_condition_for_path_value(key, start, start_op, for_project)
                end_cond =self._query_condition_for_path_value(key, end, end_op, for_project)

                if not cut.invert:
                    conds.append(start_cond)
                    conds.append(end_cond)
                else:
                    conds.append({'$or':[start_cond, end_cond]})

            else:
                # one condition per path element
                for idx, p in enumerate(cut.path):
                    conds.append( self._query_condition_for_path_value(cut_hierarchy.levels[idx].key, p, "$ne" if cut.invert else '$eq', for_project) )

        elif isinstance(cut, SetCut):
            for path in cut.paths:
                path_conds = []
                for idx, p in enumerate(path):
                    path_conds.append( self._query_condition_for_path_value(cut_hierarchy.levels[idx].key, p, "$ne" if cut.invert else '$eq', for_project) )
                conds.append({ "$and" : path_conds })
            conds = [{ "$or" : conds }]
        # FIXME for multi-level range: it's { $or: [ level_above_me < value_above_me, $and: [level_above_me = value_above_me, my_level < my_value] }
        # of the level value.
        elif isinstance(cut, RangeCut):
            if is_date_dimension(cut.dimension):
                start_cond = None
                end_cond = None
                if cut.from_path:
                    start = self._build_date_for_cut(cut_hierarchy, cut.from_path)
                    if start is not None:
                        start_cond = self._query_condition_for_path_value(cut_hierarchy.levels[0].key, start, '$gte' if not cut.invert else '$lt', for_project)
                if cut.to_path:
                    end = self._build_date_for_cut(cut_hierarchy, cut.to_path, is_end=True)
                    if end is not None:
                        end_cond = self._query_condition_for_path_value(cut_hierarchy.levels[0].key, end, '$lt' if not cut.invert else '$gte', for_project)

                if not cut.invert:
                    if start_cond:
                        conds.append(start_cond)
                    if end_cond:
                        conds.append(end_cond)
                else:
                    if start_cond and end_cond:
                        conds.append({'$or':[start_cond, end_cond]})
                    elif start_cond:
                        conds.append(start_cond)
                    elif end_cond:
                        conds.append(end_cond)

            if False:
                raise ArgumentError("No support yet for non-date range cuts in mongo backend")
                if cut.from_path:
                    last_idx = len(cut.from_path) - 1
                    for idx, p in enumerate(cut.from_path):
                        op = ( ("$lt", "$ne") if cut.invert else ("$gte", "$eq") )[0 if idx == last_idx else 1]
                        conds.append( self._query_condition_for_path_value(cut.dimension, p, op, for_project))
                if cut.to_path:
                    last_idx = len(cut.to_path) - 1
                    for idx, p in enumerate(cut.to_path):
                        op = ( ("$gt", "$ne") if cut.invert else ("$lte", "$eq") )[0 if idx == last_idx else 1]
                        conds.append( self._query_condition_for_path_value(cut.dimension, p, "$gt" if cut.invert else "$lte", for_project) )
        else:
            raise ValueError("Unrecognized cut object: %r" % cut)
        return conds

    def _query_condition_for_path_value(self, attr, value, op=None, for_project=False):
        phys = self.mapper.physical(attr)
        return phys.match_expression(value, op, for_project)

    def _order_to_sort_object(self, order=None):
        """Prepares mongo sort object from `order`. `order` is expected to be
        result from `prepare_order()`"""

        if not order:
            return []

        order_by = collections.OrderedDict()
        # each item is a 2-tuple of (logical_attribute_name, sort_order_string)

        for attribute, direction in order:
            ref = attribute.ref()

            sort_order = -1 if direction == 'desc' else 1

            if ref not in order_by:
                esc = escape_level(ref)
                order_by[esc] = (esc, sort_order)

        self.logger.debug("=== ORDER: %s" % order_by)
        return dict(order_by.values())

    def test(self, aggregate=False, **options):
        """Tests whether the statement can be constructed."""
        cell = Cell(self.cube)

        attributes = self.cube.all_attributes

        facts = self.facts(cell, page=0, page_size=1)
        # TODO: do something useful with the facts result

        # TODO: this might be slow
        if aggregate:
            result = self.aggregate()


def complex_sorted(items, sortings):
    if not sortings or not items:
        return items

    idx, direction = sortings.pop(0)

    if sortings:
        items = complex_sorted(items, sortings)

    return sorted(items, key=lambda x:x.get(idx) or x['_id'].get(idx), reverse=direction in set(['reverse', 'desc', '-1', -1]))


def escape_level(ref):
    return ref.replace('.', '___')


def unescape_level(ref):
    return ref.replace('___', '.')


class MongoFactsIterator(Facts):
    def __init__(self, facts, attributes, mapper, datesupport):
        super(MongoFactsIterator, self).__init__(facts, attributes)
        self.mapper = mapper
        self.datesupport = datesupport

    def __iter__(self):
        for fact in self.facts:
            fact = to_json_safe(fact)
            fact = collapse_record(fact)

            record = {}

            for attribute in self.attributes:
                physical = self.mapper.physical(attribute)
                value = fact.get(physical.field, attribute.missing_value)

                if value and physical.is_date_part:
                    if physical.extract != "week":
                        value = getattr(value, physical.extract)
                    else:
                        value = self.datesupport.calc_week(value)

                record[attribute.ref()] = value

            yield record
