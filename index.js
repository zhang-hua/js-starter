const { MongoClient } = require("mongodb");
const uri = "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);
const db = client.db('test');
const insertresults = db.collection('insert_results');
const queryresults = db.collection('query_results');

function getCollection(vectorSize, vectorCount, bulkSize, randomVector) {
    return db.collection(`vectors_${vectorSize}_${vectorCount}_${bulkSize}_${randomVector}`);
}

var randomVector = function(size) {
    var v = [];
    for(var i = 1; i <= size; i++)
    {
        v.push(Math.random())
    }
    return v;
}

// Create a collection and insert 'vectorCount' vectors
async function bulkInsertVectors(collection, vectorSize, vectorCount, bulkSize, randomVectorCount)
{
    if (vectorCount < randomVectorCount)
    {
        var vectorCache =[];
        for (var i = 0; i < randomVectorCount; i++)
        {
            vectorCache.push(randomVector(vectorSize));
        }
        var bulk = [];
        for (var i = 1; i <= vectorCount; i++) {
            bulk.push({ "_id" : i, "value": vectorCache[i % randomVectorCount]});
            if (bulk.length == bulkSize) {
                await collection.insertMany(bulk);
                bulk = [];
            }
        }
    }
    else
    {
        for (var i = 1; i <= vectorCount; i++) {
            await collection.insertOne({ "_id" : i, "value": randomVector(vectorSize)});
        }
    }
}

async function testVectorInsertion(vectorSizeDim, vectorCountDim, bulkSizeDim, randomVectorCountDim)
{
    for (var m = 0; m < vectorSizeDim.length; m++)
    {
        for (var n = 0; n < vectorCountDim.length; n++)
        {
            for (var i = 0; i < bulkSizeDim.length; i++)
            {
                for (var j = 0; j < randomVectorCountDim.length; j++)
                {
                    var collection = getCollection(vectorSizeDim[m], vectorCountDim[n], bulkSizeDim[i], randomVectorCountDim[j]);
                    const collections = await db.listCollections().toArray()
                    if (collections.find(c => c.name == collection.collectionName))
                    {
                        await collection.drop();
                    }
                    var randomVectorCount = Math.min(vectorCountDim[n], randomVectorCountDim[j]);
                    var bulkSize = Math.min(vectorCountDim[n], bulkSizeDim[i]);
                    var startTime = new Date();
                    await bulkInsertVectors(collection, vectorSizeDim[m], vectorCountDim[n], bulkSize, randomVectorCount);
                    var endTime = new Date();
                    await insertresults.insertOne({"collectionName": collection.collectionName, "vectorSize": vectorSizeDim[m], "vectorCount": vectorCountDim[n], "bulkSize": bulkSize, "randomVectorCount": randomVectorCount, "startTime": startTime, "endTime": endTime, "durationMs": endTime - startTime });
                }
            }
        }
    }

}

async function queryCollection(collection, calculation, sorting, top)
{
    var result = [];
    var docs = [];
    if (calculation)
    {
        docs.push({
            $project: {
                cosine: {
                    $reduce: {
                        input: { $range: [ 0, { $size: "$value" }] },
                        initialValue: 0,
                        in: { $add: [ "$$value", { $multiply: [ { $arrayElemAt: [ "$value", "$$this" ] }, { $arrayElemAt: [ "$$inputVector", "$$this" ] } ] } ] }
                    }
                }
            }
        });

        if (sorting)
        {
            docs.push({
                $sort: { cosine: -1 }
            });
        }
    }

    docs.push({   $limit: top   });
    result = await collection.aggregate(docs, { let: { inputVector: randomVector(1024) } }).toArray();
    return result;
}

async function testVectorQuery(vectorSizeDim, vectorCountDim, bulkSizeDim, randomVectorCountDim, calculationDim, sortingDim, topDim)
{
    for (var m = 0; m < vectorSizeDim.length; m++)
    {
        for (var n = 0; n < vectorCountDim.length; n++)
        {
            for (var i = 0; i < bulkSizeDim.length; i++)
            {
                for (var j = 0; j < randomVectorCountDim.length; j++)
                {
                    var collection = getCollection(vectorSizeDim[m],vectorCountDim[n], bulkSizeDim[i], randomVectorCountDim[j]);
                    for (var k = 0; k < sortingDim.length; k++)
                    {
                        for (var l = 0; l < topDim.length; l++)
                        {
                            for (var x = 0; x < calculationDim.length; x++)
                            {
                                var startTime = new Date();
                                var result = await queryCollection(collection, calculationDim[x], sortingDim[k], topDim[l]);
                                var endTime = new Date();
                                await queryresults.insertOne({"collectionName": collection.collectionName, "bulkSize": bulkSizeDim[i], "randomVectorCount": randomVectorCountDim[j], "calculation": calculationDim[x], "sorting": sortingDim[k], "top": topDim[l],  "startTime": startTime, "endTime": endTime, "durationMs": endTime - startTime, "resultCount": result.length });
                            }
                        }
                    }
                }
            }
        }
    }
}

async function main()  {
    try
    {
        var bulkSizeDim = [1, 10, 100, 1000, 10000];
        var randomVectorCountDim = [1, 10, 100, 1000, 100000];
        var calculationDim = [true, false];
        var sortingDim = [true, false];
        var topDim = [10, 100, 1000];
        var vectorSizeDim = [1024];
        var vectorCountDim = [100000];
        await testVectorInsertion(vectorSizeDim, vectorCountDim, bulkSizeDim, randomVectorCountDim);
        await testVectorQuery(vectorSizeDim, vectorCountDim, bulkSizeDim, randomVectorCountDim, calculationDim, sortingDim, topDim);
    }
    catch (error)
    {
        console.error(error);
    }
    finally
    {
        // Ensures that the client will close when you finish/error
        await client.close();
    }
}

main();
