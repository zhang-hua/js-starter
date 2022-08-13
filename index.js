const fs = require("fs");
const { MongoClient } = require("mongodb");
const { exit } = require("process");
const uri = "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);
const db = client.db('test');

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

async function testVectorInsertion(vectorOption, insertionOption)
{
    const vectorSizeDim = vectorOption.sizeDim;
    const vectorCountDim = vectorOption.countDim;
    const bulkSizeDim = insertionOption.bulkSizeDim;
    const randomVectorCountDim = insertionOption.randomVectorCountDim;
    const insertResults = db.collection(insertionOption.outputCollection);

    for (var m = 0; m < vectorSizeDim.length; m++)
    {
        for (var n = 0; n < vectorCountDim.length; n++)
        {
            for (var i = 0; i < bulkSizeDim.length; i++)
            {
                for (var j = 0; j < randomVectorCountDim.length; j++)
                {
                    const collections = await db.listCollections().toArray();
                    var collection = getCollection(vectorSizeDim[m], vectorCountDim[n], bulkSizeDim[i], randomVectorCountDim[j]);
                    if (collections.find(c => c.name == collection.collectionName))
                    {
                        // Recreate collection if it doesn't exist, otherwise reuse existing collection and skip insertion.
                        if (!insertionOption.recreateCollection)
                        {
                            console.debug("Skipping recreate collection: ", collection.collectionName);
                            continue;
                        }
                        await collection.drop();
                    }

                    var randomVectorCount = Math.min(vectorCountDim[n], randomVectorCountDim[j]);
                    var bulkSize = Math.min(vectorCountDim[n], bulkSizeDim[i]);
                    var startTime = new Date();
                    await bulkInsertVectors(collection, vectorSizeDim[m], vectorCountDim[n], bulkSize, randomVectorCount);
                    var endTime = new Date();
                    const storageSize = (await collection.stats()).storageSize;
                    const idoc = {"collectionName": collection.collectionName, "vectorSize": vectorSizeDim[m], "vectorCount": vectorCountDim[n], "bulkSize": bulkSize, "randomVectorCount": randomVectorCount, "startTime": startTime, "endTime": endTime, "durationMs": endTime - startTime, "storageSize": storageSize };
                    console.debug("The insert result: ", idoc);
                    await insertResults.insertOne(idoc);
                }
            }
        }
    }
}

async function queryCollection(collection, vectorSize, calculation, sorting, top)
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
                        in: { $add: [ "$$value", { $multiply: [ { $arrayElemAt: [ "$value", "$$this" ] }, { $arrayElemAt: [ "$$queryVector", "$$this" ] } ] } ] }
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
    result = await collection.aggregate(docs, { let: { queryVector: randomVector(vectorSize) } }).toArray();
    return result;
}

async function testVectorQuery(vectorOption, insertionOption, queryOption)
{
    const vectorSizeDim = vectorOption.sizeDim;
    const vectorCountDim = vectorOption.countDim;
    const bulkSizeDim = insertionOption.bulkSizeDim;
    const randomVectorCountDim = insertionOption.randomVectorCountDim;
    const calculationDim = queryOption.calculationDim;
    const sortingDim = queryOption.sortingDim;
    const topDim = queryOption.topDim;
    const queryResults = db.collection(queryOption.outputCollection);

    for (var m = 0; m < vectorSizeDim.length; m++)
    {
        for (var n = 0; n < vectorCountDim.length; n++)
        {
            for (var i = 0; i < bulkSizeDim.length; i++)
            {
                for (var j = 0; j < randomVectorCountDim.length; j++)
                {
                    var collection = getCollection(vectorSizeDim[m], vectorCountDim[n], bulkSizeDim[i], randomVectorCountDim[j]);
                    for (var k = 0; k < sortingDim.length; k++)
                    {
                        for (var l = 0; l < topDim.length; l++)
                        {
                            for (var x = 0; x < calculationDim.length; x++)
                            {
                                var startTime = new Date();
                                var result = await queryCollection(collection, vectorSizeDim[m], calculationDim[x], sortingDim[k], topDim[l]);
                                var endTime = new Date();
                                const storageSize = (await collection.stats()).storageSize;
                                const qdoc = {"collectionName": collection.collectionName,"vectorSize": vectorSizeDim[m], "vectorCount": vectorCountDim[n],  "bulkSize": bulkSizeDim[i], "randomVectorCount": randomVectorCountDim[j], "calculation": calculationDim[x], "sorting": sortingDim[k], "top": topDim[l],  "startTime": startTime, "endTime": endTime, "durationMs": endTime - startTime, "resultCount": result.length, "storageSize": storageSize };
                                console.debug("The query result: ", qdoc);
                                await queryResults.insertOne(qdoc);
                            }
                        }
                    }
                }
            }
        }
    }
}

async function exportInsertionResult(insertionOption)
{
    const insertResults = db.collection(insertionOption.outputCollection);
    const path = insertionOption.export.path;
    if (!insertionOption.export.overwrite && fs.existsSync(path))
    {
        console.error(`The file '${path}' already exists, skipped!`);
        return;
    }
    fs.writeFileSync(path, 'collectionName,vectorSize,vectorCount,bulkSize,randomVectorCount,startTime,endTime,durationMs,storageSize\n');
    await insertResults.find().forEach(doc => {
        try
        {
            const line = `${doc.collectionName},${doc.vectorSize},${doc.vectorCount},${doc.bulkSize},${doc.randomVectorCount},${doc.startTime},${doc.endTime},${doc.durationMs},${doc.storageSize}\n`;
            fs.appendFileSync(path, line);
        }
        catch(error)
        {
            console.error(error);
        }
    });
}

async function exportQueryResult(queryOption)
{
    const queryResults = db.collection(queryOption.outputCollection);
    const path = queryOption.export.path;
    if (!queryOption.export.overwrite && fs.existsSync(path))
    {
        console.error(`The file '${path}' already exists, skipped!`);
        return;
    }
    fs.writeFileSync(path, 'collectionName,vectorSize,vectorCount,bulkSize,randomVectorCount,calculation,sorting,top,resultCount,startTime,endTime,durationMs,storageSize\n');
    await queryResults.find().forEach(doc => {
        try
        {
            const line = `${doc.collectionName},${doc.vectorSize},${doc.vectorCount},${doc.bulkSize},${doc.randomVectorCount},${doc.calculation},${doc.sorting},${doc.top},${doc.resultCount},${doc.startTime},${doc.endTime},${doc.durationMs},${doc.storageSize}\n`;
            fs.appendFileSync(path, line);
        }
        catch(error)
        {
            console.error(error);
        }
    });
}

async function run(testSet)
{
    try
    {
        for(var i = 0; i < testSet.execution.count; i++){
            await testVectorInsertion(testSet.vector, testSet.insertion);
            await testVectorQuery(testSet.vector, testSet.insertion, testSet.query);
            await sleep(testSet.execution.intervalInSeconds * 1000);
        }
        await exportInsertionResult(testSet.insertion);
        await exportQueryResult(testSet.query);
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

async function main(args)  {
    var path = "./testset_100k_1024.json";
    if (args[0])
    {
        path = args[0];
    }

    try {
        var file = fs.readFileSync(path);
        const testSet = JSON.parse(file);
        await run(testSet);
    }
    catch (error)
    {
        console.error(error);
        return;
    }
}

main(process.argv.slice(2));
