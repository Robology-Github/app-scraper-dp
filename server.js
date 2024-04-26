// Description: This file contains the server-side code for the app. It uses the Express.js framework to create a server that listens for requests on port 3000. The server has two routes: /search and /collection. The /search route fetches app details for a search term from the App Store and Google Play, while the /collection route fetches app details for a collection of apps from the App Store and Google Play. The app details include app metadata and reviews. The server also uploads the app details to Google Cloud Storage and runs Python scripts to transform the data before uploading it.
import express from "express";
import bodyParser from "body-parser";
import googlePlay from "google-play-scraper";
import appStore from "app-store-scraper";
import fsPromises from "fs/promises";
import { Parser } from "json2csv";
import dotenv from "dotenv";
import { Storage } from "@google-cloud/storage";
import path from "path";
import { spawn } from "child_process";
import { count } from "console";

dotenv.config();

// Google Cloud Storage credentials
const credentials = {
  type: process.env.TYPE,
  project_id: process.env.PROJECT_ID,
  private_key_id: process.env.PRIVATE_KEY_ID,
  private_key: process.env.PRIVATE_KEY.split(String.raw`\n`).join("\n"),
  client_email: process.env.CLIENT_EMAIL,
  client_id: process.env.CLIENT_ID,
  auth_uri: process.env.AUTH_URI,
  token_uri: process.env.TOKEN_URI,
  auth_provider_x509_cert_url: process.env.AUTH_PROVIDER_X509_CERT_URL,
  client_x509_cert_url: process.env.CLIENT_X509_CERT_URL,
};

// Create an Express app
const app = express();
const port = process.env.PORT || 3000;
app.use(express.static("public"));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// Google Cloud Storage configuration
const storage = new Storage({
  credentials,
});
const bucketName = process.env.GCS_BUCKET_NAME;
const folderPath = process.env.GCS_FOLDER_PATH;

// CSV parser configuration
const parser = new Parser({
  delimiter: ",",
  quote: '"',
  escape: '"',
});

// Start the server
app.listen(port, () => {
  console.log(`App listening at http://localhost:${port}`);
  console.log('Python path:', process.env.PYTHON_PATH || "python3");

});

// Routes
app.get("/search", async (req, res) => {
  const { term, country, num } = req.query;

  if (!term || !country || !num) {
    return res.status(400).send("Missing term or num parameter");
  }

  try {
    const results = await searchFetchAppDetails(term, country, num);
    res.json(results);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error fetching app details");
  }
});

app.get("/collection", async (req, res) => {
  const { collection, country, num } = req.query;

  if (!collection || !country || !num) {
    return res
      .status(400)
      .send("Missing collection, country, or num parameter");
  }

  try {
    // Decide which function to call based on the platform parameter
    const results = await collectionFetchAppDetails(collection, country, num);
    res.json(results);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error fetching app details");
  }
});

// Route to get similar apps based on an app name
app.get("/similar", async (req, res) => {
  const { appName, country } = req.query;

  if (!appName || !country) {
    return res.status(400).send("Missing appName or country parameter");
  }

  try {
    const results = await similarFetchAppDetails(appName, country);
    res.json(results);
  } catch (error) {
    console.error("Failed to fetch similar apps:", error);
    res.status(500).send("Error fetching similar apps");
  }
});

// Function to fetch app details for a collection of apps
async function collectionFetchAppDetails(
  collectionList,
  countryList,
  collectionNumResults
) {
  const collectionIOS = collectionList + "_IOS";
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

    // Fetch reviews for each app in the App Store collection
    const appStoreReviewsPromises = collectionResultsAppStore.map((app) =>
      fetchAppStoreReviews(app.appId, countryList)
        .then((reviews) => {
          app.reviews = reviews; // Append reviews to the app object
          return app;
        })
        .catch((error) => {
          console.error(
            `Failed to fetch App Store reviews for ${app.appId}: ${error}`
          );
          app.reviews = "Failed to fetch reviews";
          return app;
        })
    );

    // Fetch reviews for each app in the Google Play collection
    const googlePlayReviewsPromises = collectionResultsGooglePlay.map((app) =>
      fetchGooglePlayReviews(app.appId, countryList)
        .then((reviews) => {
          app.reviews = reviews; // Append reviews to the app object
          return app;
        })
        .catch((error) => {
          console.error(
            `Failed to fetch Google Play reviews for ${app.appId}: ${error}`
          );
          app.reviews = "Failed to fetch reviews";
          return app;
        })
    );

    // Wait for all reviews to be fetched
    const updatedCollectionResultsAppStore = await Promise.all(
      appStoreReviewsPromises
    );
    const updatedCollectionResultsGooglePlay = await Promise.all(
      googlePlayReviewsPromises
    );

    const csvAppStore = parser.parse(updatedCollectionResultsAppStore);
    const csvGooglePlay = parser.parse(updatedCollectionResultsGooglePlay);

    await fsPromises.writeFile("GooglePlayOutput.csv", csvGooglePlay);
    console.log("Successfully wrote to CSV Google Play file");

    await fsPromises.writeFile("AppStoreOutput.csv", csvAppStore);
    console.log("Successfully wrote to CSV Appstore file");

    executePythonScript(
      "transform_GooglePlayData",
      "./GooglePlayOutput.csv",
      "./GooglePlayOutput_cleaned.csv"
    )
      .then(() => {
        // Upload file to GCS after Python script completes
        console.log("Now uploading to GCS...");
        uploadFileToGCS("GooglePlayOutput_cleaned.csv", bucketName, folderPath)
          .then(() =>
            console.log(
              "GooglePlayOutput_cleaned.csv successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error(
              "Failed to upload GooglePlayOutput_cleaned.csv:",
              error
            )
          );
        uploadFileToGCS("GooglePlay_Categories.csv", bucketName, folderPath)
          .then(() =>
            console.log("GooglePlay_Categories successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload GooglePlay_Categories:", error)
          );

        uploadFileToGCS("GooglePlay_Bigrams.csv", bucketName, folderPath)
          .then(() =>
            console.log("GooglePlay_Bigrams successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload GooglePlay_Bigrams.csv:", error)
          );

        uploadFileToGCS(
          "GooglePlay_Word_Frequencies.csv",
          bucketName,
          folderPath
        )
          .then(() =>
            console.log(
              "GooglePlay_Word_Frequencies successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error(
              "Failed to upload GooglePlay_Word_Frequencies.csv:",
              error
            )
          );
      })
      .catch((error) => {
        console.error("Failed to execute Python script:", error);
      });

    executePythonScript(
      "transform_AppStoreData",
      "./AppStoreOutput.csv",
      "./AppStoreOutput_cleaned.csv"
    ) 
      .then(() => {
        // Upload file to GCS after Python script completes
        console.log("Now uploading to GCS...");
        uploadFileToGCS("AppStoreOutput_cleaned.csv", bucketName, folderPath)
          .then(() =>
            console.log(
              "AppStoreOutput_cleaned.csv successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error("Failed to upload AppStoreOutput_cleaned.csv:", error)
          );

        uploadFileToGCS("AppStore_Genres.csv", bucketName, folderPath)
          .then(() =>
            console.log("AppStore_Genres.csv successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload AppStore_Genres.csv:", error)
          );

        uploadFileToGCS("AppStore_Languages.csv", bucketName, folderPath)
          .then(() =>
            console.log("AppStore_Languages.csv successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload AppStore_Languages.csv:", error)
          );

        uploadFileToGCS("AppStore_Bigrams.csv", bucketName, folderPath)
          .then(() =>
            console.log("AppStore_Bigrams.csv successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload AppStore_Bigrams.csv:", error)
          );

        uploadFileToGCS("AppStore_Word_Frequencies.csv", bucketName, folderPath)
          .then(() =>
            console.log(
              "AppStore_Word_Frequencies.csv successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error(
              "Failed to upload AppStore_Word_Frequencies.csv:",
              error
            )
          );
      })
      .catch((error) => {
        console.error("Failed to execute Python script:", error);
      });

    return { collectionResultsAppStore, collectionResultsGooglePlay };
  } catch (error) {
    console.error("Failed to fetch app details:", error);
    throw error;
  }
}

async function searchFetchAppDetails(searchTerm, countryList, numResults) {
  try {
    const searchResultsGooglePlay = await googlePlay.search({
      term: searchTerm,
      country: countryList,
      num: numResults,
    });

    const searchResultsAppStore = await appStore.search({
      term: searchTerm,
      country: countryList,
      num: numResults,
    });

    // Fetch detailed app info and reviews for Google Play apps
    const detailedAppsGooglePlay = await Promise.all(
      searchResultsGooglePlay.map(async (app) => {
        const details = await googlePlay.app({
          appId: app.appId,
          country: countryList,
        });
        const reviews = await fetchGooglePlayReviews(app.appId, countryList);
        return { ...details, reviews }; // Include reviews in the app details
      })
    );

    // Fetch detailed app info and reviews for App Store apps
    const detailedAppsAppStore = await Promise.all(
      searchResultsAppStore.map(async (app) => {
        const details = await appStore.app({
          appId: app.appId,
          country: countryList,
        });
        const reviews = await fetchAppStoreReviews(app.appId, countryList);
        return { ...details, reviews }; // Include reviews in the app details
      })
    );

    const csvGooglePlay = parser.parse(detailedAppsGooglePlay);
    const csvAppStore = parser.parse(detailedAppsAppStore);

    await fsPromises.writeFile("AppStoreOutput.csv", csvAppStore);
    console.log("Successfully wrote to CSV Appstore file");

    await fsPromises.writeFile("GooglePlayOutput.csv", csvGooglePlay);
    console.log("Successfully wrote to CSV Google Play file");

    executePythonScript(
      "transform_GooglePlayData",
      "./GooglePlayOutput.csv",
      "./GooglePlayOutput_cleaned.csv"
    )
      .then(() => {
        // Upload file to GCS after Python script completes
        console.log("Now uploading to GCS...");
        uploadFileToGCS("GooglePlayOutput_cleaned.csv", bucketName, folderPath)
          .then(() =>
            console.log(
              "GooglePlayOutput_cleaned.csv successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error(
              "Failed to upload GooglePlayOutput_cleaned.csv:",
              error
            )
          );
        uploadFileToGCS("GooglePlay_Categories.csv", bucketName, folderPath)
          .then(() =>
            console.log("GooglePlay_Categories successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload GooglePlay_Categories:", error)
          );

        uploadFileToGCS("GooglePlay_Bigrams.csv", bucketName, folderPath)
          .then(() =>
            console.log("GooglePlay_Bigrams successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload GooglePlay_Bigrams.csv:", error)
          );

        uploadFileToGCS(
          "GooglePlay_Word_Frequencies.csv",
          bucketName,
          folderPath
        )
          .then(() =>
            console.log(
              "GooglePlay_Word_Frequencies successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error(
              "Failed to upload GooglePlay_Word_Frequencies.csv:",
              error
            )
          );
      })
      .catch((error) => {
        console.error("Failed to execute Python script:", error);
      });

    executePythonScript(
      "transform_AppStoreData",
      "./AppStoreOutput.csv",
      "./AppStoreOutput_cleaned.csv"
    )
      .then(() => {
        // Upload file to GCS after Python script completes
        console.log("Now uploading to GCS...");
        uploadFileToGCS("AppStoreOutput_cleaned.csv", bucketName, folderPath)
          .then(() =>
            console.log(
              "AppStoreOutput_cleaned.csv successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error("Failed to upload AppStoreOutput_cleaned.csv:", error)
          );

        uploadFileToGCS("AppStore_Genres.csv", bucketName, folderPath)
          .then(() =>
            console.log("AppStore_Genres.csv successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload AppStore_Genres.csv:", error)
          );

        uploadFileToGCS("AppStore_Languages.csv", bucketName, folderPath)
          .then(() =>
            console.log("AppStore_Languages.csv successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload AppStore_Languages.csv:", error)
          );

        uploadFileToGCS("AppStore_Bigrams.csv", bucketName, folderPath)
          .then(() =>
            console.log("AppStore_Bigrams.csv successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload AppStore_Bigrams.csv:", error)
          );

        uploadFileToGCS("AppStore_Word_Frequencies.csv", bucketName, folderPath)
          .then(() =>
            console.log(
              "AppStore_Word_Frequencies.csv successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error(
              "Failed to upload AppStore_Word_Frequencies.csv:",
              error
            )
          );
      })
      .catch((error) => {
        console.error("Failed to execute Python script:", error);
      });

    return { detailedAppsGooglePlay, detailedAppsAppStore };
  } catch (error) {
    console.error("Failed to fetch app details:", error);
    throw error;
  }
}

async function similarFetchAppDetails(appName, country) {
  try {
    // Initial search to find the app ID from the app name
    const searchResultsGooglePlay = await googlePlay.search({
      term: appName,
      country: country,
      num: 1,
    });

    const searchResultsAppStore = await appStore.search({
      term: appName,
      country: country,
      num: 1,
    });

    // Extract app IDs (assuming the most relevant result is the first one)
    const appIdGooglePlay = searchResultsGooglePlay[0]?.appId;
    const appIdAppStore = searchResultsAppStore[0]?.appId;

    // Fetch similar apps using the retrieved app IDs
    const similarAppsGooglePlay = appIdGooglePlay
      ? await googlePlay.similar({
          appId: appIdGooglePlay,
          country: country,
          fullDetail: true,
        })
      : [];

    const similarAppsAppStore = appIdAppStore
      ? await appStore.similar({
          appId: appIdAppStore,
          country: country,
          fullDetail: true,
        })
      : [];

    // Fetch detailed app info and reviews for similar Google Play apps
    const detailedGooglePlayApps = await Promise.all(
      similarAppsGooglePlay.map(async (app) => {
        const reviews = await fetchGooglePlayReviews(app.appId, country);
        return { ...app, reviews }; // Include reviews in the app details
      })
    );

    // Fetch detailed app info and reviews for similar App Store apps
    const detailedAppStoreApps = await Promise.all(
      similarAppsAppStore.map(async (app) => {
        const reviews = await fetchAppStoreReviews(app.appId, country);
        return { ...app, reviews }; // Include reviews in the app details
      })
    );

    // Convert results to CSV
    const csvGooglePlay = parser.parse(detailedGooglePlayApps);
    const csvAppStore = parser.parse(detailedAppStoreApps);

    await fsPromises.writeFile("AppStoreOutput.csv", csvAppStore);
    console.log("Successfully wrote to CSV Appstore file");

    await fsPromises.writeFile("GooglePlayOutput.csv", csvGooglePlay);
    console.log("Successfully wrote to CSV Google Play file");

    executePythonScript(
      "transform_GooglePlayData",
      "./GooglePlayOutput.csv",
      "./GooglePlayOutput_cleaned.csv"
    )
      .then(() => {
        // Upload file to GCS after Python script completes
        console.log("Now uploading to GCS...");
        uploadFileToGCS("GooglePlayOutput_cleaned.csv", bucketName, folderPath)
          .then(() =>
            console.log(
              "GooglePlayOutput_cleaned.csv successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error(
              "Failed to upload GooglePlayOutput_cleaned.csv:",
              error
            )
          );
        uploadFileToGCS("GooglePlay_Categories.csv", bucketName, folderPath)
          .then(() =>
            console.log("GooglePlay_Categories successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload GooglePlay_Categories:", error)
          );
        uploadFileToGCS("GooglePlay_Bigrams.csv", bucketName, folderPath)
          .then(() =>
            console.log("GooglePlay_Bigrams successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload GooglePlay_Bigrams.csv:", error)
          );

        uploadFileToGCS(
          "GooglePlay_Word_Frequencies.csv",
          bucketName,
          folderPath
        )
          .then(() =>
            console.log(
              "GooglePlay_Word_Frequencies successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error(
              "Failed to upload GooglePlay_Word_Frequencies.csv:",
              error
            )
          );
      })
      .catch((error) => {
        console.error("Failed to execute Python script:", error);
      });

    executePythonScript(
      "transform_AppStoreData",
      "./AppStoreOutput.csv",
      "./AppStoreOutput_cleaned.csv"
    )
      .then(() => {
        // Upload file to GCS after Python script completes
        console.log("Now uploading to GCS...");
        uploadFileToGCS("AppStoreOutput_cleaned.csv", bucketName, folderPath)
          .then(() =>
            console.log(
              "AppStoreOutput_cleaned.csv successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error("Failed to upload AppStoreOutput_cleaned.csv:", error)
          );

        uploadFileToGCS("AppStore_Genres.csv", bucketName, folderPath)
          .then(() =>
            console.log("AppStore_Genres.csv successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload AppStore_Genres.csv:", error)
          );

        uploadFileToGCS("AppStore_Languages.csv", bucketName, folderPath)
          .then(() =>
            console.log("AppStore_Languages.csv successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload AppStore_Languages.csv:", error)
          );

        uploadFileToGCS("AppStore_Bigrams.csv", bucketName, folderPath)
          .then(() =>
            console.log("AppStore_Bigrams.csv successfully uploaded to GCS")
          )
          .catch((error) =>
            console.error("Failed to upload AppStore_Bigrams.csv:", error)
          );

        uploadFileToGCS("AppStore_Word_Frequencies.csv", bucketName, folderPath)
          .then(() =>
            console.log(
              "AppStore_Word_Frequencies.csv successfully uploaded to GCS"
            )
          )
          .catch((error) =>
            console.error(
              "Failed to upload AppStore_Word_Frequencies.csv:",
              error
            )
          );
      })
      .catch((error) => {
        console.error("Failed to execute Python script:", error);
      });

    return { detailedGooglePlayApps, detailedAppStoreApps };
  } catch (error) {
    console.error("Failed to fetch app details:", error);
    throw error;
  }
}

// Function to fetch Google Play app reviews
async function fetchGooglePlayReviews(appId, countryList, numOfReviews = 200) {
  const reviews = await googlePlay.reviews({
    appId: appId,
    num: numOfReviews,
    country: countryList,
  });
  // Concatenate review texts into a single string
  return reviews.data.map((review) => review.text).join(" | ");
}

// Function to fetch App Store reviews
async function fetchAppStoreReviews(appId, countryList, numOfReviews = 200) {
  const reviews = await appStore.reviews({
    appId: appId,
    num: numOfReviews,
    country: countryList,
  });
  // Concatenate review texts into a single string
  return reviews.map((review) => review.text).join(" | ");
}

// Function to upload files to GCS
async function uploadFileToGCS(fileName, bucketName, folderPath) {
  try {
    // Construct the full path within the bucket
    const destinationPath = path.join(folderPath, fileName);

    await storage.bucket(bucketName).upload(fileName, {
      destination: destinationPath,
    });
    console.log(
      `${fileName} uploaded to ${bucketName} in folder ${folderPath}`
    );
  } catch (error) {
    console.error(
      `Failed to upload ${fileName} to Google Cloud Storage in folder ${folderPath}`,
      error
    );
  }
}

// Function that returns a promise which resolves when the Python script is done
function executePythonScript(functionName, inputFilePath, outputFilePath) {
  return new Promise((resolve, reject) => {
    const pythonProcess = spawn(process.env.PYTHON_PATH || "python3", [
      "transform.py",
      functionName,
      inputFilePath,
      outputFilePath,
    ]);

    pythonProcess.stdout.on("data", (data) => {
      console.log(`stdout: ${data}`);
      console.log(process.env.PYTHON_PATH);
    });

    pythonProcess.stderr.on("data", (data) => {
      console.error(`stderr: ${data.toString()}`);
      console.log(process.env.PYTHON_PATH);
    });

    pythonProcess.on("close", (code) => {
      if (code === 0) {
        console.log("Python script completed successfully");
        resolve(); // Resolve the promise upon successful completion
      } else {
        console.error("Python script failed with code " + code);
        reject(new Error("Python script failed with code " + code)); // Reject the promise on failure
      }
    });
  });
}
