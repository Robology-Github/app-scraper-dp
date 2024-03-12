
document.addEventListener('DOMContentLoaded', (event) => {
    document.getElementById('searchForm').addEventListener('submit', async function(e) {
        e.preventDefault();
        const searchTerm = document.getElementById('searchTerm').value;
        const numResults = document.getElementById('numResults').value;
      
        const response = await fetch(`/search?term=${encodeURIComponent(searchTerm)}&num=${encodeURIComponent(numResults)}`);
        const data = await response.json();
      
        //document.getElementById('results').textContent = JSON.stringify(data, null, 2);
      });
      
  });
