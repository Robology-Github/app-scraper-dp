import express from 'express';
import bodyParser from 'body-parser';
import googlePlay from 'google-play-scraper';
import appStore from 'app-store-scraper';
import fs from 'fs/promises'; // Use promises version of fs
import { Parser } from 'json2csv';
import dotenv from 'dotenv';

dotenv.config();

const parser = new Parser({
  delimiter: ';',
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
    const csvAppStore = parser.parse(detailedAppsGooglePlay);

    await fs.writeFile('GooglePlayOutput.csv', csvGooglePlay);
    console.log('Successfully wrote to CSV Google Play file');

    await fs.writeFile('AppStoreOutput.csv', csvAppStore);
    console.log('Successfully wrote to CSV Appstore file');

    return { detailedAppsGooglePlay, detailedAppsAppStore };
  } catch (error) {
    console.error('Failed to fetch app details:', error);
    throw error;
  }
}