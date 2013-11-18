var program = require('commander');
var AWS = require('aws-sdk');
AWS.config.loadFromPath('./config.json');
var dynamoDB = new AWS.DynamoDB.Client();

var iKnowTheHeaders = false;

program.version('0.0.2').option('-t, --table [tablename]', 'Add the table you want to output to csv').option('-d, --describe').option('-u, --union').parse(process.argv);

if(!program.table) {
	console.log("You must specify a table");
	program.outputHelp();
	process.exit(1);
}

if(program.describe && program.union) {
	console.log("The union parameter only applies when scanning");
	program.outputHelp();
	process.exit(1);
}

var query = {
	"TableName": program.table,
	
};


var describeTable = function(query) {

		dynamoDB.describeTable(query, function(err, data) {

			if(!err) {

				console.dir(data.Table);

			} else console.dir(err);
		});
	}


var scanDynamoDBWithUnion = function(query) {

    dynamoDB.scan(query, function(err, data) {

        var headers = {};
        var values = [];

        if(!err) {
            for(index in data.Items) {
                var item = data.Items[index]; 
                var value = {};
                for(var propertyName in item) {
                    headers[propertyName] = 1;
                    
			        var attribute = item[propertyName];

                    // S — (String) Represents a String data type
                    // N — (String) Represents a Number data type
                    // B — (Base64 Encoded String) Represents a Binary data type
                    // SS — (Array<String>) Represents a String set data type
                    // NS — (Array<String>) Represents a Number set data type
                    // BS — (Array<Base64 Encoded String>) Represents a Binary set data type

                    if(typeof attribute.S != 'undefined')      { value[propertyName] = attribute.S }
                    else if(typeof attribute.N != 'undefined') { value[propertyName] = attribute.N }
                    else if(typeof attribute.B != 'undefined') { value[propertyName] = attribute.B }
                    else { console.dir('Set datatypes are not supported') }
                }
                values.push(value);
           }

            if(data.LastEvaluatedKey) { // Result is incomplete; there is more to come.
                query.ExclusiveStartKey = data.LastEvaluatedKey;
                
                scanDynamoDBWithUnion(query);
            };
        } else console.dir(err);

        

        printoutAll( Object.keys(headers), values ); // Print out the subset of results.
    });
};

var scanDynamoDB = function(query) {

		dynamoDB.scan(query, function(err, data) {

			if(!err) {

				printout(data.Items) // Print out the subset of results.
				if(data.LastEvaluatedKey) { // Result is incomplete; there is more to come.
					query.ExclusiveStartKey = data.LastEvaluatedKey;
					scanDynamoDB(query);
				};
			} else console.dir(err);

		});
	};

function arrayToCSV(array_input) {
	var string_output = "";
	for(var i = 0; i < array_input.length; i++) {
        if(typeof array_input[i] != 'undefined' && array_input[i].length > 0) {
    		string_output += ('"' + array_input[i].replace(/"/g, '""') + '"');
        } // Empty string with no quotes for blank

		if(i != array_input.length - 1) string_output += ","
	};
	return string_output;
}

function printout(items) {
	if(!iKnowTheHeaders) {
		var headers = [];
		if(items.length > 0) {
			for(var propertyName in items[0]) headers.push(propertyName)
			console.log(arrayToCSV(headers))
			iKnowTheHeaders = true;
		}
	}

	for(index in items) {
		var values = []
		for(var propertyName in items[index]) {
			var value = (items[index][propertyName].N) ? items[index][propertyName].N : String(items[index][propertyName].S);
			values.push(value)
		}
		console.log(arrayToCSV(values))
	}
}

function printoutAll(headers, items) {
	if (!iKnowTheHeaders){
	    // Log the known headers
	    console.log(arrayToCSV(headers));
	    iKnowTheHeaders = true;
	}

	for(index in items) {
		var values = []
		for(var propertyIndex in headers) {
            var propertyName = headers[propertyIndex];
			values.push(items[index][propertyName]);
		}
		console.log(arrayToCSV(values))
	}
}


if(program.describe) describeTable(query);
else if(program.union) scanDynamoDBWithUnion(query);
else scanDynamoDB(query);
