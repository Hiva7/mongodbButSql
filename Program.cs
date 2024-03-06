using MongoDB.Bson;
using MongoDB.Driver;
using util;
using System;

namespace MongoDBApp
{
    class Program
    {
        static void Main()
        {
            // NOTE: Create a new instance of object for each database
            Database db = new("mongodb+srv://UsernameExample:PasswordExample@cluster0.4nn0wuf.mongodb.net/", "DatabaseNameExample");

            // Returns all records from myCollection as List<BsonDocument>
            db.GetCollection("myCollection");

            // Returns value of myField from all records as List<BsonValue>
            db.GetField("myCollection", "myField");

            // Returns the latest ID value inside of myCollection as int (which is also the number of records inside a collection)
            // or 0 if no records exist
            db.GetLatestID("myCollection");

            // Returns all records from myCollection matching myField and myQuery
            db.SearchRecord("myCollection", "myField", "myQuery");

            // Add in a new record to myCollection
            // Check if the collection name, field name and data types match expectedDataTypes
            db.AddRecord("myCollection",
                new BsonDocument{
                    {"Field1", new BsonString("Value1")},
                    {"Field2", new BsonDecimal128(12.98m)},
                    {"Field3", new BsonInt64(123455678)},
                }
            );

            // Add in a new record to myCollection with relationship
            // Check if myCollection1_id with the value of 1 exists inside myCollection1
            // Check if myCollection2_id with the value of 2 exists inside myCollection2
            db.AddRecord("myCollection",
                new BsonDocument{
                    {"Field1", new BsonString("Value1")},
                    {"Field2", new BsonDecimal128(12.98m)},
                    {"Field3", new BsonInt64(123455678)},
                },
                new BsonDocument{
                    {"myCollection1_id", 1},
                    {"myCollection2_id", 2},
                }
            );

            // Delete the record with id 1 inside myCollection
            // Substract all id of all recrod higher than 3 by 1
            db.DeleteRecord("myCollection", 3);

            // Edit record inside myCollection with the id 1
            // Otherwise same logic as AddRecord
            db.EditRecord("myCollection",
                1,
                new BsonDocument{
                    {"Field1", new BsonString("Value1")},
                    {"Field2", new BsonDecimal128(12.98m)},
                    {"Field3", new BsonInt64(123455678)},
                }
            );

            // Same logic as AddRecord
            db.EditRelationship("myCollection",
                1,
                new BsonDocument{
                    {"myCollection1_id", 1},
                    {"myCollection2_id", 2},
                }
            );
        }
    }
}
