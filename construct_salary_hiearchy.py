import urllib2
import json
from pprint import pprint
from pymongo import MongoClient
import os
from csv import DictReader
import datetime

MONGO_URL = os.environ.get('MONGO_URL')

if not MONGO_URL:
	MONGO_URL = "mongodb://localhost:27017"

mongo = MongoClient(host=MONGO_URL)

# Python script that constructs a hierarchy from a Mongo document with the _id key specifying the hierarchy.
# Q: could I write this more elegantly in Haskell? Absolutely, I would use filters.
# The tree representation used by the D3 code is slightly redundant. All we need is a value and children list.
# We should not differentiate between leaf nodes and internal nodes. However, since D3 binds with the DOM, this additional
# structure might be necessary.
# If I have more time, perhaps I'll rewrite it. The goal is to minimize code.

def inflate_hierarchy():
	salary_hierarchy = {'name': 'UMN Salaries', 'children': [] }

	# the levels in the tree.
	levels = ['college', 'department']

	for entry in mongo.umn.college_and_departments.find():
		entry['name'] = entry[u'_id'][levels[0]] # get the level 0 name
		entry['value'] = entry['annual_rt']

		# we delete the first level
		insert_node(salary_hierarchy, entry, levels[1:])
		# now descend the tree and insert into the hierarchy

	return salary_hierarchy

# returns the first index where the predicate holds.
# returns -1 otherwise
def find_index_with_predicate(l, f):
	for i in range(len(l)):
		if f(l[i]):
			return i

	return -1

# insert node into tree
# cur_node: the node we are currently at
# leaf: the node we are trying to insert. 
#	This will be a MongoDB row. As an example, we might have a leaf with 
#	_id = {college: "CSE", "department": "mathematics" }
#	then at
#	Level 1: 'name' = "CSE"
#	Level 2: 'name' = "Mathematics"
# The insertion algorithm is quite similar to that for binary trees. 
# levels: a list of keys into the _id parameter specifying the levels in the hierarchy. 
# Continuing the above example, we would have:
# 	levels = {'college', 'department'}
# The levels array lists all the levels *below* the cur_node. So if we are at 'college', then
# 	levels = {'department'}
def insert_node(cur_node, leaf, levels):

	# check to see if we already exist
	index = find_index_with_predicate(cur_node['children'], lambda child: child['name'] == leaf['name'])

	# if we do not already exist
	if index == -1:
		# add a new node. But we must decide if we are a leaf or an internal node.
		if len(levels):
			# we are an internal node
			new_node = {'name': leaf['name'], 'children': [] }
			cur_node['children'].append(new_node)

		else:
			# we are a leaf node so we're done
			new_node = {'name': leaf['name'], 'size': leaf['value'] }
			cur_node['children'].append(new_node)
			# bail because we don't need to descend deeper
			return

		cur_node = new_node
	else:
		# descend into the child
		cur_node = cur_node['children'][index]

	# now we descend one level
	# the command 'levels[1:]' simply drops the head of the list.
	leaf['name'] = leaf[u'_id'][levels[0]]
	insert_node(cur_node, leaf, levels[1:]) 

# try constructing the hierarchy
print json.dumps(inflate_hierarchy(), sort_keys=True, indent=1, separators=(',', ': '));