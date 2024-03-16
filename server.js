import express from 'express';
import bodyParser from 'body-parser';
import googlePlay from 'google-play-scraper';
import appStore from 'app-store-scraper';
import fsPromises from 'fs/promises';
import { Parser } from 'json2csv';
import dotenv from 'dotenv';
import { Storage } from '@google-cloud/storage';
import path from 'path';

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
app.use(express.static('public'));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// Google Cloud Storage configuration
const storage = new Storage({ keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS});
const bucketName = process.env.GCS_BUCKET_NAME;
const folderPath = process.env.GCS_FOLDER_PATH;

const parser = new Parser({
  delimiter: ',',
  quote: '"',
  escape: '"',
});

app.listen(port, () => {
  console.log(`App listening at http://localhost:${port}`);
});

app.get('/search', async (req, res) => {
  const { term, num } = req.query;
  
  if (!term || !num) {
    return res.status(400).send('Missing term or num parameter');
  }
  
  try {
    const results = await searchAndFetchAppDetails(term, num);
    res.json(results);
  } catch (error) {
    console.error(error);
    res.status(500).send('Error fetching app details');
  }
});

app.get('/collection', async (req, res) => {
  const {collection, country, num } = req.query;
  
  if (!collection || !country || !num) {
    return res.status(400).send('Missing collection, country, or num parameter');
  }
  
  try {
    // Decide which function to call based on the platform parameter
    const results = await collectionFetchAppDetails(collection, country, num);
    res.json(results);
  } catch (error) {
    console.error(error);
    res.status(500).send('Error fetching app details');
  }
});



async function collectionFetchAppDetails(collectionList, countryList, collectionNumResults) {
  const collectionIOS = collectionList+'_IOS';
  try {
    const collectionResultsAppStore = await appStore.list({
      collection: appStore.collection.collectionIOS,
      country: countryList,
      num: collectionNumResults,
      fullDetail: true,
    });
    const collectionResultsGooglePlay = await googlePlay.list({
      collection: googlePlay.collection.collectionList,
      country: countryList,
      num: collectionNumResults,
      fullDetail: true,
    });

    
    const csvAppStore = parser.parse(collectionResultsAppStore);
    const csvGooglePlay = parser.parse(collectionResultsGooglePlay);
    
    
    const detailedAppsAppStoreJSON = collectionResultsAppStore.map(JSON.stringify).join('\n')
    const detailedAppsGooglePlayJSON = collectionResultsGooglePlay.map(JSON.stringify).join('\n')
  
      
    await fsPromises.writeFile('GooglePlayOutput.json', detailedAppsGooglePlayJSON);
    console.log('Successfully wrote to JSON Google Play file');
    
    await fsPromises.writeFile('GooglePlayOutput.csv', csvGooglePlay);
    console.log('Successfully wrote to CSV Google Play file');

    await fsPromises.writeFile('AppStoreOutput.json', detailedAppsAppStoreJSON);
    console.log('Successfully wrote to JSON Appstore file');
    
    await fsPromises.writeFile('AppStoreOutput.csv', csvAppStore);
    console.log('Successfully wrote to CSV Appstore file');
    
    // Upload files to Google Cloud Storage
   // await uploadFileToGCS('GooglePlayOutput.csv', bucketName, folderPath);
    //await uploadFileToGCS('AppStoreOutput.csv', bucketName, folderPath);
    
    return { collectionResultsAppStore, collectionResultsGooglePlay };
  } catch (error) {
    console.error('Failed to fetch app details:', error);
    throw error;
  }
}


async function searchAndFetchAppDetails(searchTerm, numResults) {
  try {
    const searchResultsGooglePlay = await googlePlay.search({
      term: searchTerm,
      num: numResults,
    });
    
    const searchResultsAppStore = await appStore.search({
      term: searchTerm,
      num: numResults,
    });
    const detailedAppsPromisesGooglePlay = searchResultsGooglePlay.map(app =>
      googlePlay.app({ appId: app.appId })
    );
    
    const detailedAppsPromisesAppStore = searchResultsAppStore.map(app =>
      appStore.app({ appId: app.appId })
    );
    
    const detailedAppsGooglePlay = await Promise.all(detailedAppsPromisesGooglePlay);
    const detailedAppsAppStore = await Promise.all(detailedAppsPromisesAppStore);
    
    const csvGooglePlay = parser.parse(detailedAppsGooglePlay);
    const csvAppStore = parser.parse(detailedAppsAppStore);
    
    const detailedAppsGooglePlayJSON = detailedAppsGooglePlay.map(JSON.stringify).join('\n')
    const detailedAppsAppStoreJSON = detailedAppsAppStore.map(JSON.stringify).join('\n')

  
    
    await fsPromises.writeFile('GooglePlayOutput.json', detailedAppsGooglePlayJSON);
    console.log('Successfully wrote to JSON Google Play file');
    
    await fsPromises.writeFile('AppStoreOutput.json', detailedAppsAppStoreJSON);
    console.log('Successfully wrote to JSON Appstore file');
    
    await fsPromises.writeFile('GooglePlayOutput.csv', csvGooglePlay);
    console.log('Successfully wrote to CSV Google Play file');
    
    await fsPromises.writeFile('AppStoreOutput.csv', csvAppStore);
    console.log('Successfully wrote to CSV Appstore file');
    
    // Upload files to Google Cloud Storage
   // await uploadFileToGCS('GooglePlayOutput.csv', bucketName, folderPath);
    //await uploadFileToGCS('AppStoreOutput.csv', bucketName, folderPath);
    
    return { detailedAppsGooglePlay, detailedAppsAppStore };
  } catch (error) {
    console.error('Failed to fetch app details:', error);
    throw error;
  }
}


// Function to upload files to GCS
async function uploadFileToGCS(fileName, bucketName, folderPath) {
  try {
    // Construct the full path within the bucket
    const destinationPath = path.join(folderPath, fileName);

    await storage.bucket(bucketName).upload(fileName, {
      destination: destinationPath,
    });
    console.log(`${fileName} uploaded to ${bucketName} in folder ${folderPath}`);
  } catch (error) {
    console.error(`Failed to upload ${fileName} to Google Cloud Storage in folder ${folderPath}`, error);
  }
}

