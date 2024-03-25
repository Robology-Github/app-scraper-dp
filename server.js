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

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
app.use(express.static("public"));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// Google Cloud Storage configuration
const storage = new Storage({
  credentials: JSON.parse(process.env.SERVICE_ACCOUNT_KEY.replace(/\\n/gm, "\n"))
});
const bucketName = process.env.GCS_BUCKET_NAME;
const folderPath = process.env.GCS_FOLDER_PATH;

const parser = new Parser({
  delimiter: ",",
  quote: '"',
  escape: '"',
});

app.listen(port, () => {
  console.log(`App listening at http://localhost:${port}`);
});

app.get("/search", async (req, res) => {
  const { term, num } = req.query;

  if (!term || !num) {
    return res.status(400).send("Missing term or num parameter");
  }

  try {
    const results = await searchAndFetchAppDetails(term, num);
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
      fetchAppStoreReviews(app.appId)
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
      fetchGooglePlayReviews(app.appId)
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

    const detailedAppsAppStoreJSON = collectionResultsAppStore
      .map(JSON.stringify)
      .join("\n");
    const detailedAppsGooglePlayJSON = collectionResultsGooglePlay
      .map(JSON.stringify)
      .join("\n");

    await fsPromises.writeFile(
      "GooglePlayOutput.json",
      detailedAppsGooglePlayJSON
    );
    console.log("Successfully wrote to JSON Google Play file");

    await fsPromises.writeFile("GooglePlayOutput.csv", csvGooglePlay);
    console.log("Successfully wrote to CSV Google Play file");

    await fsPromises.writeFile("AppStoreOutput.json", detailedAppsAppStoreJSON);
    console.log("Successfully wrote to JSON Appstore file");

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

    // Fetch detailed app info and reviews for Google Play apps
    const detailedAppsGooglePlay = await Promise.all(
      searchResultsGooglePlay.map(async (app) => {
        const details = await googlePlay.app({ appId: app.appId });
        const reviews = await fetchGooglePlayReviews(app.appId);
        return { ...details, reviews }; // Include reviews in the app details
      })
    );

    // Fetch detailed app info and reviews for App Store apps
    const detailedAppsAppStore = await Promise.all(
      searchResultsAppStore.map(async (app) => {
        const details = await appStore.app({ appId: app.appId });
        const reviews = await fetchAppStoreReviews(app.appId);
        return { ...details, reviews }; // Include reviews in the app details
      })
    );

    const csvGooglePlay = parser.parse(detailedAppsGooglePlay);
    const csvAppStore = parser.parse(detailedAppsAppStore);

    const detailedAppsGooglePlayJSON = detailedAppsGooglePlay
      .map(JSON.stringify)
      .join("\n");
    const detailedAppsAppStoreJSON = detailedAppsAppStore
      .map(JSON.stringify)
      .join("\n");

    await fsPromises.writeFile(
      "GooglePlayOutput.json",
      detailedAppsGooglePlayJSON
    );

    console.log("Successfully wrote to JSON Google Play file");

    await fsPromises.writeFile("AppStoreOutput.csv", csvAppStore);
    console.log("Successfully wrote to CSV Appstore file");

    await fsPromises.writeFile("AppStoreOutput.json", detailedAppsAppStoreJSON);
    console.log("Successfully wrote to JSON Appstore file");

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

// Function to fetch Google Play app reviews
async function fetchGooglePlayReviews(appId, numOfReviews = 200) {
  const reviews = await googlePlay.reviews({
    appId: appId,
    num: numOfReviews,
  });
  // Concatenate review texts into a single string
  return reviews.data.map((review) => review.text).join(" | ");
}

// Function to fetch App Store reviews
async function fetchAppStoreReviews(appId, numOfReviews = 200) {
  const reviews = await appStore.reviews({
    appId: appId,
    num: numOfReviews,
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
    const pythonProcess = spawn(process.env.PYTHON_PATH || 'python3', [
      "transform.py",
      functionName,
      inputFilePath,
      outputFilePath,
    ]);

    pythonProcess.stdout.on("data", (data) => {
      console.log(`stdout: ${data}`);
    });

    pythonProcess.stderr.on("data", (data) => {
      console.error(`stderr: ${data.toString()}`);
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
