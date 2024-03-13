  import { google } from 'googleapis';
  import express from 'express';
  import bodyParser from 'body-parser';
  import googlePlay from 'google-play-scraper';
  import appStore from 'app-store-scraper';
  import fs from 'fs';
  import fsPromises from 'fs/promises';
  import { Parser } from 'json2csv';
  import dotenv from 'dotenv';
  import { createRequire } from 'module';
  
  dotenv.config();
  
  
  const require = createRequire(import.meta.url)
  
  const parser = new Parser({
    delimiter: ',',
    quote: '"',
    escape: '"',
  });
  
  const app = express();
  const port = process.env.PORT || 3000;
  
  app.use(express.static('public'));
  app.use(bodyParser.urlencoded({ extended: true }));
  app.use(bodyParser.json());
  
  app.listen(port, () => {
    console.log(`App listening at http://localhost:${port}`);
  });
  
  const SCOPE = ['https://www.googleapis.com/auth/drive'];
  const pkey = JSON.parse(process.env.SERVICE_ACCOUNT_KEY);
  const folderId = process.env.FOLDER_ID;
  async function authorize() {
      const auth = new google.auth.JWT(
          pkey.client_email,
          null,
          pkey.private_key,
          SCOPE
      );
      await auth.authorize();
      return google.drive({ version: 'v3', auth: auth });
  }
  
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
  
  async function searchAndFetchAppDetails(searchTerm, numResults) {
    try {
      const drive = await authorize();
  
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
  
      const detailedAppsGooglePlayJSON = JSON.stringify(detailedAppsGooglePlay, null, 2);
      const detailedAppsAppStoreJSON = JSON.stringify(detailedAppsAppStore, null, 2); 
  
      await fsPromises.writeFile('GooglePlayOutput.json', detailedAppsGooglePlayJSON);
      console.log('Successfully wrote to JSON Google Play file');
  
      await fsPromises.writeFile('AppStoreOutput.json', detailedAppsAppStoreJSON);
      console.log('Successfully wrote to JSON Appstore file');
  
      await fsPromises.writeFile('GooglePlayOutput.csv', csvGooglePlay);
      console.log('Successfully wrote to CSV Google Play file');
  
      await fsPromises.writeFile('AppStoreOutput.csv', csvAppStore);
      console.log('Successfully wrote to CSV Appstore file');
  
  
      // After saving files locally, upload them to Google Drive
      await uploadFileToDrive('GooglePlayOutput.csv', drive, folderId);
      await uploadFileToDrive('AppStoreOutput.csv', drive, folderId);
  
      return { detailedAppsGooglePlay, detailedAppsAppStore };
    } catch (error) {
      console.error('Failed to fetch app details:', error);
      throw error;
    }
  }
  // Find current file in Google Drive
  async function findFileByName(drive, fileName, folderId) {
    const response = await drive.files.list({
        q: `name='${fileName}' and '${folderId}' in parents and trashed=false`,
        spaces: 'drive',
        fields: 'files(id, name)',
    });

    return response.data.files.length > 0 ? response.data.files[0] : null;
}
  
async function uploadFileToDrive(fileName, drive, folderId) {
    const file = await findFileByName(drive, fileName, folderId);

    if (file) {
        // File exists, update it
        console.log(`File ${fileName} exists with ID: ${file.id}, updating...`);
        const response = await drive.files.update({
            fileId: file.id,
            media: {
                mimeType: 'text/csv', // or 'application/json', depending on your file type
                body: fs.createReadStream(fileName),
            },
        });
        console.log(`Updated file ${fileName} with ID: ${response.data.id}`);
    } else {
        // File does not exist, upload as new
        console.log(`File ${fileName} does not exist, uploading as new...`);
        const response = await drive.files.create({
            requestBody: {
                name: fileName,
                mimeType: 'text/csv', // or 'application/json', depending on your file type
                parents: [folderId],
            },
            media: {
                mimeType: 'text/csv', // or 'application/json', depending on your file type
                body: fs.createReadStream(fileName),
            },
        });
        console.log(`Uploaded new file ${fileName} with ID: ${response.data.id}`);
    }
}
  