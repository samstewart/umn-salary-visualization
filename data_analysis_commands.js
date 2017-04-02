// the command to generate the list of colleges and how much money they make
/* Example entry: 
{
	"_id" : ObjectId("58d5a38846418d33781312d3"),
	"deptid" : 11032,
	"empl_class" : "Temp Casual",
	"first_name" : "Richard",
	"last" : "Griffith",
	"start_date_at_u" : ISODate("2014-01-27T00:00:00Z"),
	"zdeptid_descr" : "CFANS Bioproducts & Biosys Eng",
	"comp_rate" : 22.77,
	"zdeptid" : "Z0012",
	"college_descr" : "FOOD, AGRI/NAT RSRC SCI, COLL",
	"std_hrs" : 0.25,
	"fte_percentage" : 0.01,
	"annual_rt" : 296.01,
	"job_code" : "0001",
	"department" : "Bioprod&Biosys Eng, Dept of",
	"college_code" : "TCOA",
	"job_entry_date" : ISODate("2014-08-25T00:00:00Z"),
	"job_title" : "Non-Exempt Temporary or Casual"
}

 My information:
 { "_id" : ObjectId("58cecbbd46418d0b8e6171a6"), 
 "College Code" : "TIOT", 
 "Empl Class" : "Graduate Assistants", 
 "First Name" : "Samuel", 
 "Department" : "CSENG Mathematics Admin", 
 "DeptID" : 11133, 
 "Start Date at U" : ISODate("2015-08-31T00:00:00Z"), 
 "Annual Rt" : 24939.2, 
 "Job Entry Date" : ISODate("2016-08-29T00:00:00Z"), 
 "ZDeptID Descr" : "CSENG Mathematics, School of", 
 "Job Code" : "9511", 
 "Last" : "Stewart", 
 "ZDeptID" : "Z0287", 
 "Job Title" : "Teaching Assistant", 
 "FTE Percentage" : 0.5, 
 "Comp Rate" : 23.98, 
 "Std Hrs" : 20, 
 "College Descr" : "SCIENCE/ENGINEERING, COL OF" }
*/
db.employees.mapReduce(
	function() { emit(this['College Descr'], this['Annual Rt']) },
	function(key, values) { return Array.sum( values ) },
	{
		out: "college_total_salaries",
		query: {'Annual Rt': { $gt: 9999.0 } }
	}
)

// we group into the finest level of detail: the 
// then we group into departments
db.employees.mapReduce(
	function() { emit(this['Department'], this['Annual Rt'] ) },
	function(key, values) { return Array.sum( values ) },
	{
		out: "department_total_salaries",
		query: {'College Descr': 'FOOD, AGRI/NAT RSRC SCI, COLL'}
	}
)

// We are going to construct the following hierarchy using the salary data
// College Name
//    - Department
// 	       - Job title 

db.employees.aggregate(
	[
	{ $match: { "annual_rt": { $gt: 9999.0 } } },
	{ $group: { _id: "$college_descr", total: { $sum: "$annual_rt" } } },
	{ $sort: { total: -1 } }
	])


db.employees.aggregate(
	[
	{ $match: { "annual_rt": { $gt: 9999.0 } } },
	{ $group: { _id: "$college_descr", total: { $sum: "$empl_class" } } },
	{ $sort: { total: -1 } }
	])


// script to construct the hierarchical structure of the organizations
db.employees.aggregate(
	{ $match: { "annual_rt": {$gt: 9999.0} } },
	{ $group: { _id: "$college_descr", total: { $sum: "$annual_rt" } } },
	{ $sort: { _id: 1 }}
	).map(function(college) {
		// find all the departments
		college.departments = db.employees.find({ "college_descr": college._id });

		return college;
	})

// this command adds an extra property to represent our current level in the hierarchy and then groups on that property
db.employees.aggregate(
	{
		{$match : {"annual_rt": {$gt: 9999.0 } }},
		{$project: {
			hierarchyLevel: 
			{
				$concat: ["$college_descr", " ==> ", "$department", " ==> ", "$job_title"]
			}, 
			annual_rt: 1,
			first: 1,
			last_name: 1,
			comp_rate: 1
			}
		},
		{ $out: "salary_hierarchy" }		
	})


// aggregate down to department level
db.employees.aggregate([{$match : {"annual_rt": {$gt: 9999.0 } }},{ $group: { _id: { college: "$college_descr", department: "$zdeptid_descr" }, annual_rt: {$sum : "$annual_rt"}}},{$out: "college_and_departments"}])