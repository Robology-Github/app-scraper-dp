import appStore from 'app-store-scraper';
import googlePlay from 'google-play-scraper';
import fs from 'fs'; // Import the fs module to enable file writing
import { Parser } from 'json2csv';

const parser = new Parser({
    // Specify fields if you want to filter them or change their order
    delimiter: ';', // Default, but you can change this, e.g., to '\t' for TSV
    quote: '"', // Default, but you can change this if necessary
    escape: '"', // Defines how quotes inside values should be escaped
  });

googlePlay.search({
    term: 'puzzle',
    num: 5

})
.then((results) => {
  // Convert the results to CSV
  const csv = parser.parse(results);
  
  // Write the CSV to a file
  fs.writeFile('GooglePlayOutput.csv', csv, (err) => {
    if (err) {
      console.log('Error writing to CSV file', err);
    } else {
      console.log('Successfully wrote to CSV file');
    }
  });
})
.catch(console.error);


appStore.search({
  term: 'puzzle',
  num: 5

})
.then((results) => {
  // Convert the results to CSV
  const csv = parser.parse(results);
  
  // Write the CSV to a file
  fs.writeFile('AppStore.csv', csv, (err) => {
    if (err) {
      console.log('Error writing to CSV file', err);
    } else {
      console.log('Successfully wrote to CSV file');
    }
  });
})
.catch(console.error);