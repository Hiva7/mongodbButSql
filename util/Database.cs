using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Generic;

namespace util;

public class Database
{

    private readonly IMongoDatabase database;
    private readonly MongoClient client;

    // Use this to create a dictionary
    // Field not inside a dictionary won't be checked for validation
    /* Example based on code below : 
       FieldName1 inside CollectionName1 can only have the value of Value1, Value2 and Value3
    */
    // NOTE: Not sure if this works for anything beside strings

    private readonly Dictionary<string, Dictionary<string, List<string>>> fieldValidations =
        new()
        {
            {
                "CollectionName1", 
                new Dictionary<string, List<string>>
                {
                    {
                        "FieldName1",
                        new List<string> { "Value1", "Value2", "Value3" }
                    }
                }
            },
            {
                "CollectionName2",
                new Dictionary<string, List<string>>
                {
                    {
                        "FieldName2",
                        new List<string> { "Value1", "Value2", "Value3" }
                    }
                }
            },
        };

    // Use this to validate data type and field name for collections
    // Collections not inside the expectedDataTypes cannot be created
    // Any attempt to add a new record inside a collection must have the required fields with it's respective data type
    // Can only use BsonType data types
    // Will work for arrays but not the data types inside 
    // Don't need to validate ID

    private static bool ValidateDataType(
        string myCollection,
        string fieldName,
        BsonValue fieldValue
    )
    {
        // Define the expected data types for each field in each collection
        var expectedDataTypes = new Dictionary<string, Dictionary<string, BsonType>>
        {
            {
                "Collection1",
                new Dictionary<string, BsonType>
                {
                    { "Field1", BsonType.Int64 },
                    { "Field2", BsonType.Decimal128 },
                    { "Field3", BsonType.String }
                }
            },
            {
                "Collection2",
                new Dictionary<string, BsonType>
                {
                    { "Field1", BsonType.DateTime },
                    { "Field2", BsonType.Decimal128 },
                    { "Field3", BsonType.Array }
                }
            },
        };

        if (expectedDataTypes.TryGetValue(myCollection, out var fieldDataTypes))
        {
            if (fieldDataTypes.TryGetValue(fieldName, out var expectedDataType))
            {
                return fieldValue.BsonType == expectedDataType;
            }
        }

        // If the field or collection is not defined in the expectedDataTypes dictionary, assume the data type is valid
        return true;
    }

    private bool ValidateField(string collectionName, string fieldName, string? fieldValue)
    {
        if (fieldValue == null)
        {
            return false;
        }
        if (
            fieldValidations.ContainsKey(collectionName)
            && fieldValidations[collectionName].ContainsKey(fieldName)
        )
        {
            return fieldValidations[collectionName][fieldName].Contains(fieldValue);
        }
        // If the field is not in the dictionary, it's considered valid
        return true;
    }

    public Database(string connectionString, string databaseName)
    {
        try
        {
            client = new MongoClient(connectionString);
            database = client.GetDatabase(databaseName);
            if (database is null)
            {
                throw new Exception("Database not found");
            }
        }
        catch (MongoException ex)
        {
            throw new Exception("Error connecting to the database", ex);
        }
    }

    public List<BsonDocument> GetCollection(string myCollection)
    {
        var collection = database.GetCollection<BsonDocument>(myCollection);
        if (collection is null)
        {
            throw new Exception("Collection not found");
        }
        var filter = new BsonDocument();
        var documents = collection.Find(filter).ToList();

        if (!documents.Any())
        {
            Console.WriteLine("No documents found in the collection");
        }

        return documents;
    }

    public List<BsonValue> GetField(string myCollection, string myField)
    {
        var collection = database.GetCollection<BsonDocument>(myCollection);
        if (collection is null)
        {
            throw new Exception("Collection not found");
        }
        var filter = new BsonDocument();
        var documents = collection.Find(filter).ToList();

        List<BsonValue> fieldValues = new();
        foreach (var document in documents)
        {
            if (document.Contains(myField))
            {
                fieldValues.Add(document[myField]);
            }
            else
            {
                throw new Exception("Field not found");
            }
        }

        return fieldValues;
    }

    public int GetLatestID(string myCollection)
    {
        var collection = database.GetCollection<BsonDocument>(myCollection);
        if (collection is null)
        {
            throw new Exception("Collection not found");
        }
        var filter = new BsonDocument();
        var documents = collection.Find(filter).ToList();

        BsonDocument? latestDocument = documents.LastOrDefault();
        if (latestDocument != null)
        {
            if (latestDocument.ElementCount > 1)
            {
                BsonElement secondElement = latestDocument.ElementAt(1);

                if (secondElement.Value.IsInt32)
                {
                    return secondElement.Value.AsInt32;
                }
            }
        }

        return 0;
    }

    public List<BsonDocument> SearchRecord(string myCollection, string myField, object myQuery)
    {
        var collection = database.GetCollection<BsonDocument>(myCollection);
        if (collection is null)
        {
            throw new Exception("Collection not found");
        }

        List<BsonDocument> matchedDocuments = new();
        var documents = collection.Find(new BsonDocument()).ToList(); // Get all documents

        foreach (var document in documents)
        {
            if (document.Contains(myField) && document[myField].ToString() == myQuery.ToString())
            {
                matchedDocuments.Add(document);
            }
        }

        if (matchedDocuments.Count == 0)
        {
            Console.WriteLine("No matched documents");
        }

        return matchedDocuments;
    }

    public void AddRecord(string myCollection, BsonDocument myRecord)
    {
        var collection = database.GetCollection<BsonDocument>(myCollection);
        if (collection is null)
        {
            throw new Exception("Collection not found");
        }
        var documents = collection.Find(new BsonDocument()).ToList();

        int latestId = GetLatestID(myCollection);

        if (latestId == 0)
        {
            myRecord.InsertAt(0, new BsonElement("_id", ObjectId.GenerateNewId()));
            myRecord.InsertAt(1, new BsonElement(myCollection + "_id", 1));
        }
        else
        {
            myRecord.InsertAt(0, new BsonElement("_id", ObjectId.GenerateNewId()));
            myRecord.InsertAt(1, new BsonElement(myCollection + "_id", latestId + 1));
        }

        if (documents.Any())
        {
            var firstDocument = documents.FirstOrDefault();
            if (firstDocument != null)
            {
                for (int i = 2; i < firstDocument.ElementCount; i++)
                {
                    if (firstDocument.ElementAt(i).Name != myRecord.ElementAt(i).Name)
                    {
                        throw new Exception(
                            "The names of the parameters do not match the names of the elements in the collection."
                        );
                    }
                }
            }
        }
        foreach (var element in myRecord)
        {
            if (!ValidateField(myCollection, element.Name, element.Value.ToString()))
            {
                throw new Exception($"Invalid value '{element.Value}' for field '{element.Name}'");
            }
            if (!ValidateDataType(myCollection, element.Name, element.Value))
            {
                throw new Exception($"Invalid data type for field '{element.Name}'");
            }
        }

        collection.InsertOne(myRecord);
        Console.WriteLine("New document has been added");
    }

    public void AddRecord(string myCollection, BsonDocument myRecord, BsonDocument relationship)
    {
        var collection = database.GetCollection<BsonDocument>(myCollection);
        if (collection is null)
        {
            throw new Exception("Collection not found");
        }
        var documents = collection.Find(new BsonDocument()).ToList();

        foreach (var element in relationship)
        {
            var relatedCollectionName = element.Name.EndsWith("_id")
                ? element.Name.Substring(0, element.Name.Length - 3)
                : element.Name;
            var relatedCollection = database.GetCollection<BsonDocument>(relatedCollectionName);

            if (element.Value.IsBsonArray)
            {
                foreach (var id in element.Value.AsBsonArray)
                {
                    var filter = Builders<BsonDocument>.Filter.Eq(element.Name, id);
                    var relatedDocument = relatedCollection.Find(filter).FirstOrDefault();

                    if (relatedDocument == null)
                    {
                        throw new Exception(
                            $"No document found in {relatedCollectionName} collection with {element.Name}: {id}"
                        );
                    }
                }
            }
            else
            {
                var filter = Builders<BsonDocument>.Filter.Eq(element.Name, element.Value);
                var relatedDocument = relatedCollection.Find(filter).FirstOrDefault();

                if (relatedDocument == null)
                {
                    throw new Exception(
                        $"No document found in {relatedCollectionName} collection with {element.Name}: {element.Value}"
                    );
                }
            }
        }

        int latestId = GetLatestID(myCollection);
        myRecord.AddRange(relationship);

        if (latestId == 0)
        {
            myRecord.InsertAt(0, new BsonElement("_id", ObjectId.GenerateNewId()));
            myRecord.InsertAt(1, new BsonElement(myCollection + "_id", 1));
        }
        else
        {
            myRecord.InsertAt(0, new BsonElement("_id", ObjectId.GenerateNewId()));
            myRecord.InsertAt(1, new BsonElement(myCollection + "_id", latestId + 1));
        }

        if (documents.Any())
        {
            var firstDocument = documents.FirstOrDefault();
            if (firstDocument != null)
            {
                for (int i = 2; i < firstDocument.ElementCount; i++)
                {
                    if (firstDocument.ElementAt(i).Name != myRecord.ElementAt(i).Name)
                    {
                        throw new Exception(
                            "The names of the parameters do not match the names of the elements in the collection."
                        );
                    }
                }
            }
        }
        foreach (var element in myRecord)
        {
            if (!ValidateField(myCollection, element.Name, element.Value.ToString()))
            {
                throw new Exception($"Invalid value '{element.Value}' for field '{element.Name}'");
            }
            if (!ValidateDataType(myCollection, element.Name, element.Value))
            {
                throw new Exception($"Invalid data type for field '{element.Name}'");
            }
        }

        collection.InsertOne(myRecord);
        Console.WriteLine("New document has been added");
    }

    public void DeleteRecord(string myCollection, int id)
    {
        var collection = database.GetCollection<BsonDocument>(myCollection);
        if (collection is null)
        {
            throw new Exception("Collection not found");
        }

        var firstDocument = collection.Find(new BsonDocument()).FirstOrDefault();
        if (firstDocument is null)
        {
            throw new Exception("No documents found in the collection");
        }

        var idName = firstDocument.Names.ElementAt(1);

        var filter = Builders<BsonDocument>.Filter.Eq(idName, id);

        var document = collection.Find(filter).FirstOrDefault();
        if (document is null)
        {
            throw new Exception("Document not found");
        }

        collection.DeleteOne(filter);

        var updateFilter = Builders<BsonDocument>.Filter.Gt(idName, id);
        var update = Builders<BsonDocument>.Update.Inc(idName, -1);
        collection.UpdateMany(updateFilter, update);
        Console.WriteLine("Record has been deleted");
    }

    public void EditRecord(string myCollection, int id, BsonDocument myRecord)
    {
        var collection = database.GetCollection<BsonDocument>(myCollection);
        if (collection is null)
        {
            throw new Exception("Collection not found");
        }
        foreach (var element in myRecord)
        {
            if (element.Name.EndsWith("_id"))
            {
                throw new Exception("myRecord should not contain a field that ends with '_id'");
            }
        }

        var filter = Builders<BsonDocument>.Filter.Eq(myCollection + "_id", id);
        var document = collection.Find(filter).FirstOrDefault();
        if (document == null)
        {
            throw new Exception(
                $"No document found in {myCollection} collection with {myCollection}_id: {id}"
            );
        }

        var update = Builders<BsonDocument>.Update;
        var updateDefinition = new List<UpdateDefinition<BsonDocument>>();

        foreach (var element in myRecord)
        {
            if (!document.Contains(element.Name))
            {
                throw new Exception($"Field '{element.Name}' does not exist in the document");
            }
            if (!ValidateField(myCollection, element.Name, element.Value.ToString()))
            {
                throw new Exception($"Invalid value '{element.Value}' for field '{element.Name}'");
            }
            if (!ValidateDataType(myCollection, element.Name, element.Value))
            {
                throw new Exception($"Invalid data type for field '{element.Name}'");
            }
            updateDefinition.Add(update.Set(element.Name, element.Value));
        }

        var result = collection.UpdateOne(filter, update.Combine(updateDefinition));

        if (result.MatchedCount == 0)
        {
            throw new Exception(
                $"No document found in {myCollection} collection with {myCollection}_id: {id}"
            );
        }

        Console.WriteLine("Document has been updated");
    }

    public void EditRelationship(string myCollection, int id, BsonDocument relationship)
    {
        var collection = database.GetCollection<BsonDocument>(myCollection);
        if (collection is null)
        {
            throw new Exception("Collection not found");
        }

        var filter = Builders<BsonDocument>.Filter.Eq(myCollection + "_id", id);
        var document = collection.Find(filter).FirstOrDefault();
        if (document == null)
        {
            throw new Exception(
                $"No document found in {myCollection} collection with {myCollection}_id: {id}"
            );
        }

        var update = Builders<BsonDocument>.Update;
        var updates = new List<UpdateDefinition<BsonDocument>>();

        foreach (var element in relationship)
        {
            if (!element.Name.EndsWith("_id"))
            {
                throw new Exception($"Foreign key '{element.Name}' does not end with '_id'");
            }
            if (element.Name != myCollection + "_id")
            {
                var relatedCollectionName = element.Name.Substring(0, element.Name.Length - 3);
                var relatedCollection = database.GetCollection<BsonDocument>(relatedCollectionName);

                if (element.Value.IsBsonArray)
                {
                    var array = element.Value.AsBsonArray;
                    var newArray = new BsonArray();
                    foreach (var value in array)
                    {
                        var intValue = value.AsInt32;
                        var relatedFilter = Builders<BsonDocument>.Filter.Eq(
                            element.Name,
                            intValue
                        );
                        var relatedDocument = relatedCollection
                            .Find(relatedFilter)
                            .FirstOrDefault();

                        if (relatedDocument == null)
                        {
                            throw new Exception(
                                $"No document found in {relatedCollectionName} collection with {element.Name}: {intValue}"
                            );
                        }
                        newArray.Add(intValue);
                    }
                    updates.Add(update.Set(element.Name, newArray));
                }
                else
                {
                    var relatedFilter = Builders<BsonDocument>.Filter.Eq(
                        element.Name,
                        element.Value
                    );
                    var relatedDocument = relatedCollection.Find(relatedFilter).FirstOrDefault();

                    if (relatedDocument == null)
                    {
                        throw new Exception(
                            $"No document found in {relatedCollectionName} collection with {element.Name}: {element.Value}"
                        );
                    }
                    if (!ValidateDataType(myCollection, element.Name, element.Value))
                    {
                        throw new Exception($"Invalid data type for field '{element.Name}'");
                    }
                    updates.Add(update.Set(element.Name, element.Value));
                }
            }
        }

        if (updates.Count > 0)
        {
            var updateDefinition = update.Combine(updates);
            collection.UpdateOne(filter, updateDefinition);
        }
    }
}
