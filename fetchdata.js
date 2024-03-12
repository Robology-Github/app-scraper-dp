import googlePlay from 'google-play-scraper';

async function searchAndFetchAppDetails(searchTerm, numResults) {
    try {
      // Use the `numResults` parameter to control the number of search results
      const searchResults = await googlePlay.search({
        term: searchTerm,
        num: numResults // Dynamically set based on function input
      });

      const detailedAppsPromises = searchResults.map(app =>
        googlePlay.app({ appId: app.appId })
      );

      const detailedApps = await Promise.all(detailedAppsPromises);

      console.log(detailedApps);
    } catch (error) {
      console.error('Failed to fetch app details:', error);
    }
}
